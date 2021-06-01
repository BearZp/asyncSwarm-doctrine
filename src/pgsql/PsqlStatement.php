<?php

namespace AsyncSwarm\Doctrine\pgsql;

use AsyncSwarm\Doctrine\Exception\AlreadyExecutedException;
use AsyncSwarm\Doctrine\Exception\EmptyResultException;
use AsyncSwarm\Doctrine\Exception\NotFoundException;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\SQLAnywhere\SQLAnywhereException;
use Doctrine\DBAL\Driver\Statement as StatementInterface;
use Doctrine\DBAL\ParameterType;
use IteratorAggregate;
use Exception;

class PsqlStatement implements IteratorAggregate, StatementInterface, Result
{
    protected $sql;

    /** @var resource */
    protected $dbConn;

    /** @var resource */
    protected $result;

    /** @var int */
    protected $rowCursor = 0;

    /** @var int */
    protected $rowTotal = 0;

    /** @var string */
    protected $statementName;

    /** @var int */
    protected $fetchMode = \PDO::FETCH_ASSOC;

    /** @var DriverConnection */
    protected $connection;

    /** @var array */
    protected $boundParams = [];

    /**
     * PsqlStatement constructor.
     * @param $sql
     * @param $dbConn
     * @param string $statementName
     * @param DriverConnection $connection
     */
    public function __construct($sql, $dbConn, string $statementName, DriverConnection $connection)
    {
        $this->sql = $sql;
        $this->dbConn = $dbConn;
        $this->statementName = $statementName;
        $this->connection = $connection;
    }

    /**
     * @throws SQLAnywhereException
     */
    protected function throwError()
    {
        if ($error = pg_result_error($this->result)) {
            $error = $error . ' Executed SQL: ' . $this->sql;
            $this->free(true);
            throw new SQLAnywhereException($error, pg_result_error_field($this->result, PGSQL_DIAG_SQLSTATE));
        } else {
            $this->free(true);
            throw new Exception('Undefined sql exception', 500);
        }
    }

    /**
     * @throws EmptyResultException
     * @throws NotFoundException
     * @throws SQLAnywhereException
     */
    protected function checkResult(): void
    {
        if (!is_resource($this->result)) {
            $this->result = pg_get_result($this->dbConn);
            if (!$this->result) {
                $this->free(true);
                throw new EmptyResultException($this->sql);
            }
            if ($error = pg_result_error($this->result)) {
                $error = $error . ' Executed SQL: ' . $this->sql;
                $this->free(true);
                throw new SQLAnywhereException($error, pg_result_error_field($this->result, PGSQL_DIAG_SQLSTATE));
            }
            $this->rowTotal = pg_num_rows($this->result);
            if ($this->rowTotal === 0 && strpos(strtolower($this->sql), 'select') === 0) {
                $this->free(true);
                throw new NotFoundException($this->sql);
            }
        }
    }

    /**
     * @return array|false|mixed
     * @throws EmptyResultException
     * @throws NotFoundException
     * @throws SQLAnywhereException
     */
    public function fetchNumeric()
    {
        $this->checkResult();
        if ($this->rowCursor === $this->rowTotal) {
            $this->free();
            return false;
        }
        $result = pg_fetch_row($this->result, $this->rowCursor);
        $this->rowCursor++;
        return $result;
    }

    /**
     * @return array|false
     */
    public function fetchAssociative()
    {
        $this->checkResult();
        if ($this->rowCursor === $this->rowTotal) {
            $this->free();
            return false;
        }
        $result = pg_fetch_assoc($this->result, $this->rowCursor);
        $this->rowCursor++;
        return $result;
    }

    /**
     * @return array|false|mixed
     */
    public function fetchOne()
    {
        $this->checkResult();
        $return = pg_fetch_row($this->result);
        $this->free();
        return $return[0];
    }

    /**
     * @return array
     */
    public function fetchAllNumeric(): array
    {
        $this->checkResult();
        $return = pg_fetch_all($this->result, PGSQL_NUM);
        $this->free();
        return $return;
    }

    /**
     * @return array
     */
    public function fetchAllAssociative(): array
    {
        $this->checkResult();
        $return = pg_fetch_all($this->result, PGSQL_ASSOC);
        $this->free();
        return $return;
    }

    /**
     * @return array
     */
    public function fetchFirstColumn(): array
    {
        $this->checkResult();
        $return = pg_fetch_all_columns($this->result);
        $this->free();
        return $return;
    }

    /**
     * @param false $close
     */
    public function free($close = false): void
    {
        if (is_resource($this->result)) {
            pg_free_result($this->result);
        }
        $this->result = null;
        $this->rowTotal = 0;
        $this->rowCursor = 0;
        $this->boundParams = [];

        //var_dump('FREE: ' . get_resource_id($this->dbConn));

        $this->connection->free($this->dbConn, $close);
    }

    /**
     * @return bool|void
     */
    public function closeCursor()
    {
        $this->free();
    }

    /**
     * @return int|void
     */
    public function columnCount()
    {
        $this->checkResult();
        return pg_num_fields($this->result);
    }

    /**
     * Sets the fetch mode.
     *
     * @deprecated Use one of the fetch- or iterate-related methods.
     *
     * @param int $fetchMode
     *
     * @return void
     */
    public function setFetchMode($fetchMode, $arg2 = null, $arg3 = null)
    {
        $this->fetchMode = $fetchMode;
    }

    /**
     * {@inheritdoc}
     *
     * @deprecated Use fetchNumeric(), fetchAssociative() or fetchOne() instead.
     */
    public function fetch($fetchMode = null, $cursorOrientation = 0, $cursorOffset = 0)
    {
        if (!$fetchMode) {
            $fetchMode = $this->fetchMode;
        }

        if ($fetchMode === \PDO::FETCH_NUM) {
            return $this->fetchNumeric();
        }
        return $this->fetchAssociative();
    }

    /**
     * {@inheritdoc}
     *
     * @deprecated Use fetchAllNumeric(), fetchAllAssociative() or fetchFirstColumn() instead.
     */
    public function fetchAll($fetchMode = null, $fetchArgument = null, $ctorArgs = null)
    {
        if (!$fetchMode) {
            $fetchMode = $this->fetchMode;
        }

        if ($fetchMode === \PDO::FETCH_NUM) {
            return $this->fetchAllNumeric();
        }
        return $this->fetchAllAssociative();
    }

    /**
     * {@inheritdoc}
     *
     * @deprecated Use fetchOne() instead.
     */
    public function fetchColumn($columnIndex = 0)
    {
        return $this->fetchOne();
    }

    /**
     * @param int|string $param
     * @param mixed $value
     * @param int $type
     * @return bool
     */
    public function bindValue($param, $value, $type = ParameterType::STRING)
    {
        $this->boundParams[$param] = $value;
        return true;
    }

    public function bindParam($param, &$variable, $type = ParameterType::STRING, $length = null)
    {
        var_dump('bindParam! If you see this - tell me (gulidov.vadim@walletfactory.com) about it!');
        var_dump($param);
        var_dump($variable);
        var_dump($type);
        var_dump($length);
        var_dump($this->sql);
        return true;
    }

    /**
     * @return bool|int|string|null
     */
    public function errorCode()
    {
        if (is_resource($this->result)) {
            return pg_result_error_field($this->result, PGSQL_DIAG_SQLSTATE);
        }
        return false;
    }

    /**
     * @return mixed[]|string
     */
    public function errorInfo()
    {
        if (is_resource($this->result)) {
            return pg_result_error($this->result);
        }
        return '';
    }

    /**
     * @param null $params
     * @return bool
     * @throws AlreadyExecutedException
     */
    public function execute($params = null)
    {
        if ($this->result) {
            throw new AlreadyExecutedException();
        }

        if ($params === null) {
            ksort($this->boundParams);
            $params = [];
            foreach ($this->boundParams as $param) {
                $params[] = $param;
            }
        }
        if ($params) {
            if ($this->statementName !== '') {
                $status =  pg_send_execute($this->dbConn, $this->statementName, $params);
            } else {
                $status = pg_send_query_params($this->dbConn, $this->sql, $params);
            }
        } else {
            $status = pg_send_query($this->dbConn, $this->sql);
        }

        if (!$status) {
            $this->checkResult();
        }

        return $status;
    }

    /**
     * @return false|int|mixed
     * @throws SQLAnywhereException
     */
    public function rowCount()
    {
        $this->checkResult();
        $return = pg_affected_rows($this->result);
        $this->free();
        return $return;
    }

    /**
     * @return \Generator
     */
    public function getIterator()
    {
        while (($result = $this->fetch()) !== false) {
            yield $result;
        }
        yield null;
    }
}
