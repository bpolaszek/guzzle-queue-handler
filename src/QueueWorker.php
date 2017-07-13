<?php

namespace BenTools\GuzzleQueueHandler;

use GuzzleHttp\ClientInterface;
use function GuzzleHttp\Psr7\parse_request;
use function GuzzleHttp\Psr7\str;
use function GuzzleHttp\json_decode;
use Interop\Queue\PsrConnectionFactory;
use Interop\Queue\PsrConsumer;
use Interop\Queue\PsrContext;
use Interop\Queue\PsrMessage;
use Interop\Queue\PsrQueue;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use SplObserver;

class QueueWorker implements \SplSubject
{

    const STATE_REQUEST = 1;
    const STATE_RESPONSE = 2;

    /**
     * @var PsrContext
     */
    private $context;

    /**
     * @var PsrQueue
     */
    private $queue;

    /**
     * @var ClientInterface
     */
    private $guzzle;

    /**
     * @var float
     */
    private $timeout;

    /**
     * @var \SplObserver[]
     */
    private $observers = [];

    /**
     * @var RequestInterface
     */
    private $currentRequest;

    /**
     * @var ResponseInterface
     */
    private $lastResponse;

    /**
     * @var int
     */
    private $state;

    /**
     * EnqueuePromisor constructor.
     * @param PsrContext      $context
     * @param string          $queueName
     * @param ClientInterface $guzzle
     * @param float           $timeout
     * @throws \InvalidArgumentException
     */
    public function __construct(PsrContext $context, ClientInterface $guzzle, $queueName = '', $timeout = 0.0)
    {
        $this->context = $context;
        if ('' === $queueName) {
            throw new \InvalidArgumentException("Queue name must not be blank.");
        }
        $this->guzzle = $guzzle;
        $this->queue = $context->createQueue($queueName);
        $this->timeout = (float) $timeout;
    }

    /**
     * @param PsrConnectionFactory $factory
     * @param string               $queueName
     * @param ClientInterface      $guzzle
     * @param float                $timeout
     * @return QueueWorker
     */
    public static function factory(PsrConnectionFactory $factory, ClientInterface $guzzle, $queueName = '', $timeout = 0.0)
    {
        return new static($factory->createContext(), $guzzle, $queueName, $timeout);
    }

    /**
     * @param null                    $maxRequests
     * @param \DateTimeImmutable|null $endAt
     */
    public function loop($maxRequests = null, \DateTimeImmutable $endAt = null)
    {
        $nbRequests = 0;
        $consumer = $this->context->createConsumer($this->queue);
        while (true) {
            if (0.0 === $this->timeout) {
                while (null === ($message = $consumer->receive())) {
                    usleep(10000);
                }
            } else {
                $max = microtime(true) + $this->timeout;
                while (null === ($message = $consumer->receive($this->timeout)) && microtime(true) <= $max) {
                    usleep(10000);
                }
            }

            // Process the message
            $this->process($message, $consumer);

            // If max requests reached
            if (null !== $maxRequests) {
                $nbRequests++;
                if ($nbRequests >= $maxRequests) {
                    break;
                }
            }

            // If maximum time reached
            if (null !== $endAt && time() >= $endAt->format('U')) {
                break;
            }
        }
    }

    /**
     * @return RequestInterface
     */
    public function getCurrentRequest()
    {
        return $this->currentRequest;
    }

    /**
     * @return ResponseInterface
     */
    public function getLastResponse()
    {
        return $this->lastResponse;
    }

    /**
     * @return int
     */
    public function getState()
    {
        return $this->state;
    }

    /**
     * @param PsrMessage  $message
     * @param PsrConsumer $consumer
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    private function process(PsrMessage $message, PsrConsumer $consumer)
    {
        /** @var RequestInterface $request */
        try {
            list($request, $options) = $this->unwrap($message);
            $request = parse_request($request);
        } catch (\InvalidArgumentException $e) {
            $message->setProperty('error', $e->getMessage());
            $consumer->reject($message);
            return;
        }
        $replyTo = $message->getReplyTo();
        if (!$replyTo) {
            $message->setProperty('error', 'replyTo was not filled.');
            $consumer->reject($message);
            return;
        }

        $options['handler'] = $this->guzzle->getConfig('handler');
        $options['synchronous'] = true;
        $options['http_errors'] = false;

        $this->lastResponse = null;

        try {
            $this->state = self::STATE_REQUEST;
            $this->currentRequest = $request;
            $this->notify();

            $response = $this->guzzle->send($request, $options);

            $this->state = self::STATE_RESPONSE;
            $this->lastResponse = $response;
            $this->notify();


            $queue = $this->context->createQueue($replyTo);
            $producer = $this->context->createProducer();
            $producer->send($queue, $this->context->createMessage(str($response)));
            $consumer->acknowledge($message);
        } catch (\Throwable $e) {
            $message->setProperty('error', $e->getMessage());
            $consumer->reject($message);
        } catch (\Exception $e) {
            $message->setProperty('error', $e->getMessage());
            $consumer->reject($message);
        }
    }

    /**
     * @param PsrMessage $message
     * @return array
     */
    private function unwrap(PsrMessage $message)
    {
        $json = json_decode($message->getBody(), true);
        return $json;
    }

    /**
     * @inheritDoc
     */
    public function attach(SplObserver $observer)
    {
        $this->observers[] = $observer;
    }

    /**
     * @inheritDoc
     */
    public function detach(SplObserver $observer)
    {
        foreach ($this->observers as $o => $_observer) {
            if ($_observer === $observer) {
                unset($this->observers[$o]);
            }
        }
    }

    /**
     * @inheritDoc
     */
    public function notify()
    {
        if (!empty($this->observers)) {
            foreach ($this->observers as $observer) {
                $observer->update($this);
            }
        }
    }
}
