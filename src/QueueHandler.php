<?php

namespace BenTools\GuzzleQueueHandler;

use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Promise\Promise;
use function GuzzleHttp\Psr7\parse_response;
use function GuzzleHttp\Psr7\str;
use Interop\Queue\PsrConnectionFactory;
use Interop\Queue\PsrContext;
use Interop\Queue\PsrQueue;
use Psr\Http\Message\RequestInterface;

class QueueHandler
{
    /**
     * @var PsrContext
     */
    private $context;

    /**
     * @var PsrQueue
     */
    private $queue;
    /**
     * @var int
     */
    private $timeout;


    /**
     * EnqueuePromisor constructor.
     * @param PsrContext $context
     * @param string     $queueName
     * @param float      $timeout
     * @throws \InvalidArgumentException
     */
    public function __construct(PsrContext $context, $queueName = '', $timeout = 0.0)
    {
        $this->context = $context;
        if ('' === $queueName) {
            throw new \InvalidArgumentException("Queue name must not be blank.");
        }
        $this->queue = $context->createQueue($queueName);
        $this->timeout = (float) $timeout;
    }

    /**
     * @param PsrConnectionFactory $factory
     * @param string               $queueName
     * @param float                $timeout
     * @return QueueHandler
     */
    public static function factory(PsrConnectionFactory $factory, $queueName = '', $timeout = 0.0)
    {
        $context = $factory->createContext();
        return new static($context, $queueName, $timeout);
    }

    /**
     * @param RequestInterface $request
     * @return Promise
     * @throws \Interop\Queue\Exception
     * @throws \Interop\Queue\InvalidDestinationException
     * @throws \Interop\Queue\InvalidMessageException
     * @throws \InvalidArgumentException
     */
    public function __invoke(RequestInterface $request, array $options)
    {
        $message = $this->wrap($request, $options);
        $replyTo = $this->queue->getQueueName() . strtr(uniqid(null, true), '.', 0);
        $message->setReplyTo($replyTo);
        $this->context->createProducer()->send($this->queue, $message);
        $promise = new Promise(function () use (&$promise, $replyTo, $request) {
            try {
                $queue = $this->context->createQueue($replyTo);
                $consumer = $this->context->createConsumer($queue);


                if (0.0 === $this->timeout) {
                    while (null === ($message = $consumer->receive())) {
                        usleep(100);
                    }
                } else {
                    $max = microtime(true) + $this->timeout;
                    while (null === ($message = $consumer->receive($this->timeout)) && microtime(true) <= $max) {
                        usleep(100);
                    }
                }

                if (null === $message) {
                    throw new RequestException('Timeout reached.', $request);
                }

                if (null !== $message->getProperty('error')) {
                    throw RequestException::create($request, null, new \RuntimeException($message->getProperty('error')));
                }
                $consumer->acknowledge($message);
                $response = parse_response($message->getBody());
                $promise->resolve($response);
            } catch (\Exception $e) {
                $promise->reject($e);
            }
        });
        return $promise;
    }

    /**
     * @param RequestInterface $request
     * @param array            $options
     * @return \Interop\Queue\PsrMessage
     * @throws \InvalidArgumentException
     */
    private function wrap(RequestInterface $request, array $options)
    {
        $message = $this->context->createMessage(json_encode([
            str($request->withRequestTarget((string) $request->getUri())),
            $options,
        ]));
        return $message;
    }
}
