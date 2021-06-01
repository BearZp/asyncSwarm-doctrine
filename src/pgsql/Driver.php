<?php

namespace AsyncSwarm\Doctrine\pgsql;

use Doctrine\DBAL\ConnectionException;
use Doctrine\DBAL\Driver\AbstractPostgreSQLDriver;
use Doctrine\DBAL\Exception;

class Driver extends AbstractPostgreSQLDriver
{
    /**
     * $params[
     *      'host',
     *      'port',
     *      'dbname',
     *      'charset',
     *      'sslmode',
     *      'sslrootcert',
     *      'sslcert',
     *      'sslkey',
     *      'sslcrl',
     *      'application_name'
     * ]
     *
     * @param array $params
     * @param null $username
     * @param null $password
     * @param array $driverOptions
     * @return \Doctrine\DBAL\Driver\Connection
     */
    public function connect(array $params, $username = null, $password = null, array $driverOptions = []): \Doctrine\DBAL\Driver\Connection
    {
        try {
            $connectionString = $this->buildConnectionString($params, $username, $password);
            $maxConnections = $driverOptions['maxConnections'] ?? $params['maxConnections'] ?? 10;
            return new DriverConnection($connectionString, $maxConnections);
        } catch (Exception $e) {
            $host = $params['host'] ?? '127.0.0.1';
            $port = $params['port'] ?? '5432';
            throw new ConnectionException(
                'An exception occurred in driver: ' . $e->getMessage() .
                'Connection refused\n\tIs the server running on host \"' . $host .
                '\" and accepting\n\tTCP/IP connections on port ' . $port . '?\"',
                $e
            );
        }
    }

    /**
     * @param array $params
     * @param string|null $username
     * @param string|null $password
     * @return string
     */
    private function buildConnectionString(array $params, string $username = null, string $password = null): string
    {
        $conn_string = '';
        if (isset($params['host'])) {
            $conn_string = $this->addParamToString(
                $conn_string,
                'host',
                $params['host']
            );
        }

        if (isset($params['port'])) {
            $conn_string = $this->addParamToString(
                $conn_string,
                'port',
                $params['port']
            );
        }

        if (isset($params['dbname'])) {
            $conn_string = $this->addParamToString(
                $conn_string,
                'dbname',
                $params['dbname']
            );
        }

        if ($username !== null) {
            $conn_string = $this->addParamToString(
                $conn_string,
                'user',
                $username
            );
        }

        if ($password !== null) {
            $conn_string = $this->addParamToString(
                $conn_string,
                'password',
                $password
            );
        }

        if (isset($params['charset'])) {
            $conn_string = $this->addParamToString(
                $conn_string,
                'options',
                '--client_encoding=' . $params['charset']
            );
        }

        return $conn_string;
    }

    /**
     * @param $conn_string
     * @param $key
     * @param $val
     * @return string
     */
    private function addParamToString($conn_string, $key, $val)
    {
        return $conn_string . ($conn_string ? ' ' : '') . $key . '=' . $val;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return 'pgsql';
    }
}