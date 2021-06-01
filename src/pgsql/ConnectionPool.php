<?php

namespace AsyncSwarm\Doctrine\pgsql;

use Doctrine\DBAL\ConnectionException;
use Doctrine\DBAL\Exception;

class ConnectionPool
{
    /** @var array */
    protected $connectionPool = [];

    /** @var array */
    protected $poolMap = [];

    /** @var array  */
    protected $stmtMap = [];

    protected $liveTimeMap = [];

    /** @var int */
    protected $maxConnection;

    /** @var integer */
    protected $liveTime = 1;

    /** @var string */
    protected $connectionString;

    /** @var resource */
    protected $defaultConnection;

    private const CONNECTION_FREE = 1,
        CONNECTION_BUSY = 2;

    /**
     * Connection constructor.
     * @param string $connectionString
     * @param int|null $maxConnections
     * @throws Exception
     */
    public function __construct(string $connectionString, int $maxConnections, int $liveTime = 10)
    {
        $this->maxConnection = $maxConnections;
        $this->liveTime = $liveTime;
        $this->connectionString = $connectionString;
        $this->defaultConnection = $this->createNewConnection();
    }

    /**
     * @return resource
     * @throws Exception
     */
    private function createNewConnection()
    {
        $connection = pg_connect($this->connectionString, PGSQL_CONNECT_FORCE_NEW);
        if ($connection === false) {
            throw new Exception('pg_connect: could not connect to server: ', 500);
        }
        return $connection;
    }

    /**
     * @param false $getDefault
     * @return mixed
     * @throws ConnectionException
     * @throws Exception
     */
    public function getConnection($getDefault = false)
    {
        if ($getDefault) {
            return $this->defaultConnection;
        }

        foreach ($this->poolMap as $connectionNumber => $connectionStatus) {
            if ($connectionStatus === self::CONNECTION_FREE &&
                is_resource($this->connectionPool[$connectionNumber]) &&
                !pg_connection_busy($this->connectionPool[$connectionNumber])
            ) {
                $connection = $this->connectionPool[$connectionNumber];
                $this->poolMap[$connectionNumber] = self::CONNECTION_BUSY;
                $this->liveTimeMap[$connectionNumber] = microtime(true);
                return $connection;
            }
        }

        $connection = $this->createNewConnection();
        $connectionNumber = get_resource_id($connection);
        $this->connectionPool[$connectionNumber] = $connection;
        $this->poolMap[$connectionNumber] = self::CONNECTION_BUSY;
        $this->liveTimeMap[$connectionNumber] = microtime(true);
        return $connection;

        //throw new ConnectionException('Max connection reached');
    }

    /**
     * @param $conn
     * @param false $close
     * @return bool
     */
    public function freeConnection(&$conn, $close = false): bool
    {
        $connectionNumber = get_resource_id($conn);
        $currentTime = microtime(true);
        if (count($this->connectionPool) > $this->maxConnection) {
            $close = true;
        }
        if ($close) {
            $this->liveTimeMap[$connectionNumber] -= $this->maxConnection;
        } else {
            $this->liveTimeMap[$connectionNumber] = $currentTime;
        }
        $this->poolMap[$connectionNumber] = self::CONNECTION_FREE;

        // clean pool
        foreach ($this->liveTimeMap as $number => $startTime) {
            if ($startTime < $currentTime - $this->liveTime &&
                count($this->connectionPool) > $this->maxConnection &&
                $this->poolMap[$number] == self::CONNECTION_FREE
            ) {
                pg_close($conn);
                unset($this->stmtMap[$connectionNumber]);
                unset($this->poolMap[$connectionNumber]);
                unset($this->connectionPool[$connectionNumber]);
                unset($this->liveTimeMap[$connectionNumber]);
            }
        }

        return true;
    }

    /**
     * @param string $stmt
     * @param resource $connection
     * @return bool
     */
    public function stmtExist(string $stmt, $connection): bool
    {
        return isset($this->stmtMap[get_resource_id($connection)][$stmt]);
    }

    /**
     * @param string $stmt
     * @param $connection
     */
    public function registerStmt(string $stmt, $connection): void
    {
        $this->stmtMap[get_resource_id($connection)][$stmt] = $stmt;
    }
}
