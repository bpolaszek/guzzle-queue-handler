<?php

require __DIR__.'/../vendor/autoload.php';

use BenTools\GuzzleQueueHandler\QueueWorker;
use Enqueue\Fs\FsConnectionFactory;
use GuzzleHttp\Client;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

(new Application('worker', '1.0.0'))
    ->register('worker')
    ->addArgument('queue', InputArgument::REQUIRED, 'The queue')
    ->setCode(function(InputInterface $input, OutputInterface $output) {
        $factory = new FsConnectionFactory(sys_get_temp_dir());
        $context = $factory->createContext();
        $consumer = new QueueWorker($context, new Client(), $input->getArgument('queue'));
        $consumer->loop();
    })
    ->getApplication()
    ->setDefaultCommand('worker', true)
    ->run();