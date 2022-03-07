<?php

namespace ZnDatabase\Doctrine\Domain\Base;

//use Illuminate\Database\Query\Builder as QueryBuilder;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\PDOStatement;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use ZnCore\Domain\Helpers\EntityHelper;
use ZnCore\Domain\Interfaces\GetEntityClassInterface;
use ZnDatabase\Base\Domain\Traits\TableNameTrait;

abstract class BaseDoctrineRepository implements GetEntityClassInterface
{

    use TableNameTrait;

    protected $autoIncrement = 'id';
    private $capsule;

    public function __construct(Connection $capsule)
    {
        $this->capsule = $capsule;
    }

    public function autoIncrement()
    {
        return $this->autoIncrement;
    }

    /*public function getCapsule(): Connection
    {
        return $this->capsule;
    }*/

    public function getConnection(): Connection
    {
        return $this->capsule;
        //$connection = $this->capsule->getConnection();
        //return $connection;
    }

    public function encodeTableName(string $sourceTableName): string
    {
        return $sourceTableName;
        /*$tableAlias = $this->getCapsule()->getAlias();
        $targetTableName = $tableAlias->encode($this->connectionName(), $sourceTableName);
        return $targetTableName;*/
    }

    protected function tableNameForQuery(): string
    {
        return 'c';
    }

    protected function getQueryBuilder(): QueryBuilder
    {
        $connection = $this->getConnection();
        $queryBuilder = $connection->createQueryBuilder()
            ->from($this->tableNameAlias(), $this->tableNameForQuery())
            ->select('*');
        //$queryBuilder = $connection->table($this->tableNameAlias(), null, $this->connectionName());
        return $queryBuilder;
    }

    protected function getSchema(string $connectionName = null): AbstractSchemaManager
    {
        $connection = $this->getConnection($connectionName);
        $schema = $connection->getSchemaManager();
        return $schema;
    }

    protected function allByBuilder(QueryBuilder $queryBuilder)
    {
        $connection = $this->getConnection();
        $array = $connection->fetchAll($queryBuilder->getSQL());

        //$postCollection = $queryBuilder->();
        //$array = $postCollection->toArray();
        //return $this->forgeEntityCollection($array);

        $entityClass = $this->getEntityClass();
        return EntityHelper::createEntityCollection($entityClass, $array);
    }

    protected function countByBuilder(QueryBuilder $queryBuilder): int
    {
        $connection = $this->getConnection();
        $queryBuilder->select('COUNT(*) as count');
        return $connection->executeQuery($queryBuilder->getSQL())->fetchColumn(0);
    }

    protected function executeQuery(QueryBuilder $queryBuilder): PDOStatement {
        $connection = $this->getConnection();
        return $connection->executeQuery($queryBuilder->getSQL());
    }

    /*public function getEntityClass(): string
    {
        return $this->entityClass;
    }*/

    /*protected function oneByBuilder(QueryBuilder $queryBuilder)
    {
        $item = $queryBuilder->first();
        if (empty($item)) {
            throw new NotFoundException('Not found entity!');
        }
        return $this->forgeEntity($item);
    }*/

}
