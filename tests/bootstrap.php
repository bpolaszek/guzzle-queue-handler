<?php

use Symfony\Component\Process\ProcessBuilder;

require __DIR__.'/../vendor/autoload.php';

dump("Loading worker...");
$process = (new ProcessBuilder())->setPrefix('php')->setArguments(['tests/worker.php', 'test'])->getProcess();
$process->start();