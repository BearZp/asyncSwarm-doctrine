<?php

namespace AsyncSwarm\Doctrine\Exception;

use Exception;

class AlreadyExecutedException extends Exception
{
    public function __construct()
    {
        parent::__construct('You trying to execute already executed statement', 500);
    }
}
