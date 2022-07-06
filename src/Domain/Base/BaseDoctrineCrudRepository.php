<?php

namespace ZnDatabase\Doctrine\Domain\Base;

use Doctrine\DBAL\Driver\PDOStatement;
use Doctrine\DBAL\Query\QueryBuilder;
use Illuminate\Database\QueryException;
use ZnCore\Collection\Interfaces\Enumerable;
use ZnCore\Contract\Common\Exceptions\InvalidMethodParameterException;
use ZnCore\Query\Enums\OperatorEnum;
use ZnCore\Validation\Exceptions\UnprocessibleEntityException;
use ZnCore\Entity\Helpers\EntityHelper;
use ZnCore\Entity\Interfaces\EntityIdInterface;
use ZnCore\Repository\Interfaces\CrudRepositoryInterface;
//use ZnCore\Repository\Interfaces\RelationConfigInterface;
use ZnCore\Query\Entities\Query;
use ZnCore\Entity\Exceptions\NotFoundException;
use ZnCore\Arr\Helpers\ArrayHelper;
use ZnDatabase\Doctrine\Domain\Helpers\QueryBuilder\DoctrineQueryBuilderHelper;
use ZnCore\Relation\Libs\QueryFilter;

abstract class BaseDoctrineCrudRepository extends BaseDoctrineRepository implements CrudRepositoryInterface//, RelationConfigInterface
{

    protected $primaryKey = ['id'];

    public function primaryKey()
    {
        return $this->primaryKey;
    }

    protected function forgeQuery(Query $query = null): Query
    {
        $query = Query::forge($query);
        return $query;
    }

    protected function queryFilterInstance(Query $query = null)
    {
        $query = $this->forgeQuery($query);
        /** @var QueryFilter $queryFilter */
        $queryFilter = new QueryFilter($this, $query);
        return $queryFilter;
    }

    public function count(Query $query = null): int
    {
        $query = $this->forgeQuery($query);
        $queryBuilder = $this->getQueryBuilder();
        DoctrineQueryBuilderHelper::setWhere($query, $queryBuilder);
        return $this->countByBuilder($queryBuilder);
    }

    public function _all(Query $query = null)
    {
        $query = $this->forgeQuery($query);
        $queryBuilder = $this->getQueryBuilder();
        DoctrineQueryBuilderHelper::setWhere($query, $queryBuilder);
        DoctrineQueryBuilderHelper::setSelect($query, $queryBuilder);
        DoctrineQueryBuilderHelper::setOrder($query, $queryBuilder);
        DoctrineQueryBuilderHelper::setPaginate($query, $queryBuilder);
        $collection = $this->allByBuilder($queryBuilder);
        return $collection;
    }

    public function findAll(Query $query = null): Enumerable
    {
        $query = $this->forgeQuery($query);
        $queryFilter = $this->queryFilterInstance($query);
//        $queryWithoutRelations = $queryFilter->getQueryWithoutRelations();
        $queryWithoutRelations = $query;
        $collection = $this->_all($queryWithoutRelations);
        $collection = $queryFilter->loadRelations($collection);
        return $collection;
    }

    public function findOneById($id, Query $query = null): EntityIdInterface
    {
        if(empty($id)) {
            throw (new InvalidMethodParameterException('Empty ID'))
                ->setParameterName('id');
        }
        $query = $this->forgeQuery($query);
        $query->where('id', $id);
        return $this->findOne($query);
    }

    public function findOne(Query $query = null): object
    {
        $query->limit(1);
        $collection = $this->findAll($query);
        if ($collection->count() < 1) {
            throw new NotFoundException('Not found entity!');
        }
        return $collection->first();
    }

    private function getLastId($query) {
        $conn = $this->getConnection();
        $stmt = $conn->prepare($query);
        $stmt->execute();
        $lastId = $stmt->fetch()['id'];
        return $lastId;
    }

    public function create(EntityIdInterface $entity)
    {

        $columnList = $this->getColumnsForModify();
        $arraySnakeCase = EntityHelper::toArrayForTablize($entity, $columnList);

        $queryBuilder = $this->getQueryBuilder();

        foreach ($arraySnakeCase as $key => &$item) {
            if($item instanceof \DateTime) {
                $item = $item->format('Y-m-d H:i:s');
            }
            //$item = $queryBuilder->createNamedParameter($item);
        }

        try {
            //print_r($arraySnakeCase);exit;
            $queryBuilder = $queryBuilder
                ->insert($this->tableNameAlias())
                ->values($arraySnakeCase);

            $lastId = $this->executeQuery($queryBuilder);
            //print_r($lastId);exit;
            //print_r($lastId);exit;
            $entity->setId($lastId);
        } catch (QueryException $e) {
            $errors = new UnprocessibleEntityException;
            $errors->add('', 'Already exists!');
            throw $errors;
        }
    }

    private function getColumnsForModify()
    {
        $schema = $this->getSchema();
        $columnList = $schema->listTableColumns($this->tableNameAlias());
        $columnList = array_keys($columnList);
        if ($this->autoIncrement()) {
            ArrayHelper::removeByValue($this->autoIncrement(), $columnList);
        }
        return $columnList;
    }

    /*public function persist(EntityIdInterface $entity)
    {

    }*/

    public function update(EntityIdInterface $entity)
    {
        $this->findOneById($entity->getId());
        $data = EntityHelper::toArrayForTablize($entity);
        $this->updateQuery($entity->getId(), $data);
        //$this->updateById($entity->getId(), $data);
    }

    /*public function updateById($id, $data)
    {
        $this->findOneById($id);
        $this->updateQuery($id, $data);
    }*/

    private function updateQuery($id, array $data)
    {
        $columnList = $this->getColumnsForModify();
        $data = ArrayHelper::extractByKeys($data, $columnList);
        $queryBuilder = $this->getQueryBuilder();
        $queryBuilder->find($id);
        $queryBuilder->update($data);
    }

    public function deleteById($id)
    {
        $entity = $this->findOneById($id);
        $queryBuilder = $this->getQueryBuilder();
        $predicates = $queryBuilder->expr()->andX();
        $predicates->add($queryBuilder->expr()->eq('id', $entity->getId()));
        $this->deleteByPredicates($predicates, $queryBuilder);
    }

    public function deleteByCondition(array $condition)
    {
        $queryBuilder = $this->getQueryBuilder();
        $predicates = $queryBuilder->expr()->andX();
        foreach ($condition as $key => $value) {
            $predicates->add($queryBuilder->expr()->eq($key, $value));
        }
        $this->deleteByPredicates($predicates, $queryBuilder);
    }

    private function deleteByPredicates($predicates, QueryBuilder $queryBuilder): PDOStatement {
        $queryBuilder = $queryBuilder
            ->delete($this->tableNameAlias())
            ->where($predicates);
        return $this->executeQuery($queryBuilder);
    }

}