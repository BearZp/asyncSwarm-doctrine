<?php

namespace AsyncSwarm\Doctrine\pgsql;

use Doctrine\DBAL\Driver\SQLAnywhere\SQLAnywhereException;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\ParameterType;

class DriverConnection implements \Doctrine\DBAL\Driver\Connection
{
    /** @var ConnectionPool */
    protected $connectionPool;

    /**
     * @var resource
     */
    protected $defaultConnection;

    /**
     * @var bool
     */
    protected $isTransaction = false;

    /**
     * Connection constructor.
     * @param string $connectionString
     * @param int|null $maxConnections
     * @throws Exception
     */
    public function __construct(string $connectionString, int $maxConnections)
    {
        $this->connectionPool = new ConnectionPool($connectionString, $maxConnections);
        $this->defaultConnection = $this->connectionPool->getConnection(true);
    }

    /**
     * @param string $sql
     * @return PsqlStatement
     * @throws Exception
     * @throws \Doctrine\DBAL\ConnectionException
     */
    public function prepare($sql): PsqlStatement
    {
        $dbConnection = $this->connectionPool->getConnection($this->isTransaction);
        $statementName = '';

        if (strpos($sql, '$1')) {
            $statementName = md5($sql);
            if (!$this->connectionPool->stmtExist($statementName, $dbConnection)) {
                //prepare sql
                pg_send_prepare($dbConnection, $statementName, $sql);
                //clean connection
                $result = pg_get_result($dbConnection);
                $error = pg_result_error($result);
                if ($error) {
                    $error = $error . ' Executed SQL: ' . $sql;
                    $this->free($dbConnection, true);
                    throw new SQLAnywhereException($error, pg_result_error_field($result, PGSQL_DIAG_SQLSTATE));
                }
                $this->connectionPool->registerStmt($statementName, $dbConnection);
            }
        }
        return new PsqlStatement($sql, $dbConnection, $statementName, $this);
    }

    /**
     * @return PsqlStatement|Statement
     * @throws Exception
     * @throws \Doctrine\DBAL\ConnectionException
     */
    public function query()
    {
        $args = func_get_args();
        $stmt = $this->prepare($args[0]);

        $stmt->execute();

        return $stmt;
    }

    /**
     * @param mixed $value
     * @param int $type
     * @return int|mixed|string
     * @throws Exception
     * @throws \Doctrine\DBAL\ConnectionException
     */
    public function quote($value, $type = ParameterType::STRING)
    {
        $connection = $this->connectionPool->getConnection();
        switch ($type) {
            case ParameterType::LARGE_OBJECT :
            case ParameterType::ASCII :
            case ParameterType::STRING : {
                $result = pg_escape_literal($connection, $value);
            }
            break;
            case ParameterType::INTEGER : {
                $result = (int) $value;
            }
            break;
            case ParameterType::BINARY : {
                $result = pg_escape_bytea($connection, $value);
            }
            break;
            case ParameterType::BOOLEAN : {
                $result = $value ? 'TRUE' : 'FALSE';
            }
            break;
            case ParameterType::NULL : {
                $result = 'NULL';
            }
            break;
            default: {
                throw new Exception('Unsupported parameter type: ' . $type, 500);
            }
        }
        return $result;
    }

    /**
     * @param string $sql
     * @return int
     * @throws Exception
     * @throws \Doctrine\DBAL\ConnectionException
     */
    public function exec($sql)
    {
        $connection = $this->connectionPool->getConnection($this->isTransaction);
        $result = pg_query($connection, $sql);
        return pg_affected_rows($result);
    }

    public function lastInsertId($name = null)
    {
        throw new \Exception('Method not implemented');
    }

    /**
     * @return bool
     * @throws Exception
     * @throws \Doctrine\DBAL\ConnectionException
     */
    public function beginTransaction()
    {
        $this->isTransaction = true;
        $connection = $this->connectionPool->getConnection($this->isTransaction);
        if (!pg_query($connection, 'BEGIN')) {
            throw new \Exception('Could not start transaction');
        }

        return true;
    }

    /**
     * @return bool
     * @throws Exception
     * @throws \Doctrine\DBAL\ConnectionException
     */
    public function commit()
    {
        $connection = $this->connectionPool->getConnection($this->isTransaction);
        $this->isTransaction = false;
        if (!pg_query($connection, 'COMMIT')) {
            throw new \Exception('Transaction commit failed');
        }
        return true;
    }

    /**
     * @return bool
     * @throws Exception
     * @throws \Doctrine\DBAL\ConnectionException
     */
    public function rollBack()
    {
        $connection = $this->connectionPool->getConnection($this->isTransaction);
        $this->isTransaction = false;
        if (!pg_query($connection, 'ROLLBACK')) {
            throw new \Exception('Transaction rollback failed');
        }

        return true;
    }

    public function errorCode()
    {
        // TODO: Implement errorCode() method.
    }

    public function errorInfo()
    {
        // TODO: Implement errorInfo() method.
    }

    /**
     * @param $connection
     * @param false $close
     * @return bool
     */
    public function free($connection, $close = false)
    {
        return $this->connectionPool->freeConnection($connection, $close);
    }
}
