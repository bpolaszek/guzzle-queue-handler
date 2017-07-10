<?php

namespace BenTools\GuzzleQueueHandler\Tests;

use BenTools\GuzzleQueueHandler\QueueHandler;
use Enqueue\Fs\FsConnectionFactory;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\ResponseInterface;

class QueueTest extends TestCase
{

    public function testWithNotimeout()
    {
        $stack = HandlerStack::create(QueueHandler::factory(new FsConnectionFactory(sys_get_temp_dir()), 'test'));
        $guzzle = new \GuzzleHttp\Client([
            'handler' => $stack,
        ]);

        $request = new Request('GET', 'https://httpbin.org/get');
        $response = $guzzle->send($request);
        $this->assertInstanceOf(ResponseInterface::class, $response);
    }

    /**
     * @expectedException \GuzzleHttp\Exception\RequestException
     * @expectedExceptionMessage 400 BAD REQUEST
     */
    public function testWithErrorCode()
    {
        $stack = HandlerStack::create(QueueHandler::factory(new FsConnectionFactory(sys_get_temp_dir()), 'test'));
        $guzzle = new \GuzzleHttp\Client([
            'handler' => $stack,
        ]);

        $request = new Request('GET', 'https://httpbin.org/status/400');
        $guzzle->send($request);
    }

    /**
     * @expectedException \GuzzleHttp\Exception\RequestException
     * @expectedExceptionMessage Timeout reached.
     */
    public function testWithtimeout()
    {
        $stack = HandlerStack::create(QueueHandler::factory(new FsConnectionFactory(sys_get_temp_dir()), 'test', 0.9));
        $guzzle = new \GuzzleHttp\Client([
            'handler' => $stack,
        ]);

        $request = new Request('GET', 'https://httpbin.org/delay/1');
        $response = $guzzle->send($request);
        $this->assertInstanceOf(ResponseInterface::class, $response);
    }
}
