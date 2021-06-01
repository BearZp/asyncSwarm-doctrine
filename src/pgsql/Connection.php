<?php

namespace AsyncSwarm\Doctrine\pgsql;

use Doctrine\DBAL\Cache\QueryCacheProfile;
use Doctrine\DBAL\Driver\ResultStatement;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Types\Type;
use Throwable;

class Connection extends \Doctrine\DBAL\Connection
{
    /**
     * Executes an, optionally parametrized, SQL query.
     *
     * If the query is parametrized, a prepared statement is used.
     * If an SQLLogger is configured, the execution is logged.
     *
     * @param string                                                               $sql    SQL query
     * @param array<int, mixed>|array<string, mixed>                               $params Query parameters
     * @param array<int, int|string|Type|null>|array<string, int|string|Type|null> $types  Parameter types
     *
     * @return ResultStatement The executed statement.
     *
     * @throws Exception
     */
    public function executeQuery($sql, array $params = [], $types = [], ?QueryCacheProfile $qcp = null)
    {
        if ($qcp !== null) {
            return $this->executeCacheQuery($sql, $params, $types, $qcp);
        }
        $connection = $this->getWrappedConnection();
        $logger = $this->_config->getSQLLogger();
        if ($logger) {
            $logger->startQuery($sql, $params, $types);
        }
        try {
            if ($params) {
                [$sql, $params, $types] = SQLParser::expandListParameters($sql, $params, $types);

                $stmt = $connection->prepare($sql);
                if ($types) {
                    $this->bindTypedValues($stmt, $params, $types);
                    $stmt->execute();
                } else {
                    $stmt->execute($params);
                }
            } else {
                $stmt = $connection->prepare($sql);
                $stmt->execute();
            }
        } catch (Throwable $e) {
            $this->handleExceptionDuringQuery(
                $e,
                $sql,
                $params,
                $types
            );
        }
        $stmt->setFetchMode($this->defaultFetchMode);
        if ($logger) {
            $logger->stopQuery();
        }

        return $stmt;
    }

    /**
     * Executes an SQL statement with the given parameters and returns the number of affected rows.
     *
     * Could be used for:
     *  - DML statements: INSERT, UPDATE, DELETE, etc.
     *  - DDL statements: CREATE, DROP, ALTER, etc.
     *  - DCL statements: GRANT, REVOKE, etc.
     *  - Session control statements: ALTER SESSION, SET, DECLARE, etc.
     *  - Other statements that don't yield a row set.
     *
     * This method supports PDO binding types as well as DBAL mapping types.
     *
     * @param string                                                               $sql    SQL statement
     * @param array<int, mixed>|array<string, mixed>                               $params Statement parameters
     * @param array<int, int|string|Type|null>|array<string, int|string|Type|null> $types  Parameter types
     *
     * @return int The number of affected rows.
     *
     * @throws Exception
     */
    public function executeStatement($sql, array $params = [], array $types = [])
    {
        $connection = $this->getWrappedConnection();

        $logger = $this->_config->getSQLLogger();
        if ($logger) {
            $logger->startQuery($sql, $params, $types);
        }

        try {
            if ($params) {
                [$sql, $params, $types] = SQLParser::expandListParameters($sql, $params, $types);

                $stmt = $connection->prepare($sql);

                if ($types) {
                    $this->bindTypedValues($stmt, $params, $types);
                    $stmt->execute();
                } else {
                    $stmt->execute($params);
                }

                $result = $stmt->rowCount();
                $stmt->free();
            } else {
                $result = $connection->exec($sql);
            }
        } catch (Throwable $e) {
            $this->handleExceptionDuringQuery(
                $e,
                $sql,
                $params,
                $types
            );
        }

        if ($logger) {
            $logger->stopQuery();
        }

        return $result;
    }

    /****===================****/

    /**
     * Binds a set of parameters, some or all of which are typed with a PDO binding type
     * or DBAL mapping type, to a given statement.
     *
     * @internal Duck-typing used on the $stmt parameter to support driver statements as well as
     *           raw PDOStatement instances.
     *
     * @param \Doctrine\DBAL\Driver\Statement                                      $stmt   Prepared statement
     * @param array<int, mixed>|array<string, mixed>                               $params Statement parameters
     * @param array<int, int|string|Type|null>|array<string, int|string|Type|null> $types  Parameter types
     *
     * @return void
     */
    protected function bindTypedValues($stmt, array $params, array $types)
    {
        // Check whether parameters are positional or named. Mixing is not allowed, just like in PDO.
        if (is_int(key($params))) {
            // Positional parameters
            $typeOffset = array_key_exists(0, $types) ? -1 : 0;
            $bindIndex  = 1;
            foreach ($params as $value) {
                $typeIndex = $bindIndex + $typeOffset;
                if (isset($types[$typeIndex])) {
                    $type                  = $types[$typeIndex];
                    [$value, $bindingType] = $this->getBindingInfo($value, $type);
                    $stmt->bindValue($bindIndex, $value, $bindingType);
                } else {
                    $stmt->bindValue($bindIndex, $value);
                }

                ++$bindIndex;
            }
        } else {
            // Named parameters
            foreach ($params as $name => $value) {
                if (isset($types[$name])) {
                    $type                  = $types[$name];
                    [$value, $bindingType] = $this->getBindingInfo($value, $type);
                    $stmt->bindValue($name, $value, $bindingType);
                } else {
                    $stmt->bindValue($name, $value);
                }
            }
        }
    }

    /**
     * Gets the binding type of a given type. The given type can be a PDO or DBAL mapping type.
     *
     * @param mixed                $value The value to bind.
     * @param int|string|Type|null $type  The type to bind (PDO or DBAL).
     *
     * @return mixed[] [0] => the (escaped) value, [1] => the binding type.
     */
    protected function getBindingInfo($value, $type)
    {
        if (is_string($type)) {
            $type = Type::getType($type);
        }

        if ($type instanceof Type) {
            $value       = $type->convertToDatabaseValue($value, $this->getDatabasePlatform());
            $bindingType = $type->getBindingType();
        } else {
            $bindingType = $type;
        }

        return [$value, $bindingType];
    }
}
