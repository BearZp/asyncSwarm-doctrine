<?php

namespace AsyncSwarm\Doctrine\Exception;

use Throwable;

class EmptyResultException extends \Exception
{
    public function __construct(string $sql, Throwable $previous = null)
    {
        $message = 'Result of query undefined. Executed SQL: ' . $sql;
        parent::__construct($message, 500, $previous);
    }
}
