<?php

namespace AsyncSwarm\Doctrine\Exception;

use Throwable;

class NotFoundException extends \Exception
{
    public function __construct(string $sql, Throwable $previous = null)
    {
        $message = 'Result not found. Executed SQL: ' . $sql;
        parent::__construct($message, 404, $previous);
    }
}
