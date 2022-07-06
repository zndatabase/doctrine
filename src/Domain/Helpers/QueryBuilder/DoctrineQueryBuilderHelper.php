<?php

namespace ZnDatabase\Doctrine\Domain\Helpers\QueryBuilder;

use Illuminate\Database\Query\Builder;
use ZnDatabase\Base\Domain\Helpers\DbHelper;
use ZnDatabase\Base\Domain\Interfaces\QueryBuilderInterface;
use ZnCore\Query\Enums\OperatorEnum;
use ZnCore\Query\Entities\Query;
use ZnCore\Query\Entities\Where;
use Doctrine\DBAL\Query\QueryBuilder;

class DoctrineQueryBuilderHelper implements QueryBuilderInterface
{

    public static function setWhere(Query $query, QueryBuilder $queryBuilder)
    {
        $queryArr = $query->toArray();
        if ( ! empty($queryArr[Query::WHERE])) {
            foreach ($queryArr[Query::WHERE] as $key => $value) {

                $predicates = $queryBuilder->expr()->andX();

                if (is_array($value)) {
                    $predicates->add($queryBuilder->expr()->eq(/*'c.' . */$key, $value));
                    //$queryBuilder->whereIn($key, $value);
                } else {
                    $predicates->add($queryBuilder->expr()->in(/*'c.' . */$key, $value));
                    //$queryBuilder->where($key, $value);
                }
                $queryBuilder->where($predicates);
            }
        }

        $whereArray = $query->getWhereNew();
        if ( ! empty($whereArray)) {
            /** @var Where $where */
            foreach ($whereArray as $where) {

                $expr = $queryBuilder->expr();

                if($where->boolean == 'and') {
                    $predicates = $queryBuilder->expr()->andX();
                } elseif($where->boolean == 'or') {
                    $predicates = $queryBuilder->expr()->orX();
                }

                if (is_array($where->value)) {
                    $predicates->add($expr->in(/*'c.' .*/ $where->column, $where->value));
                    //$queryBuilder->whereIn($where->column, $where->value, $where->boolean, $where->not);
                } else {
                    if($where->operator == OperatorEnum::EQUAL) {
                        if($where->not) {
                            $predicates->add($expr->eq(/*'c.' .*/ $where->column, $where->value));
                        } else {
                            $predicates->add($expr->neq(/*'c.' .*/ $where->column, $where->value));
                        }
                    } elseif ($where->operator == OperatorEnum::NOT_EQUAL) {
                        if($where->not) {
                            $predicates->add($expr->neq(/*'c.' .*/ $where->column, $where->value));
                        } else {
                            $predicates->add($expr->eq(/*'c.' .*/ $where->column, $where->value));
                        }
                    } elseif ($where->operator == OperatorEnum::NULL) {
                        if($where->not) {
                            $predicates->add($expr->isNotNull($where->column));
                        } else {
                            $predicates->add($expr->isNull($where->column));
                        }
                    } elseif ($where->operator == OperatorEnum::NOT_NULL) {
                        if($where->not) {
                            $predicates->add($expr->isNull($where->column));
                        } else {
                            $predicates->add($expr->isNotNull($where->column));
                        }
                    } elseif ($where->operator == OperatorEnum::LESS) {
                        $predicates->add($expr->lt($where->column, $where->value));
                    } elseif ($where->operator == OperatorEnum::LESS_OR_EQUAL) {
                        $predicates->add($expr->lte($where->column, $where->value));
                    } elseif ($where->operator == OperatorEnum::GREATER) {
                        $predicates->add($expr->gt($where->column, $where->value));
                    } elseif ($where->operator == OperatorEnum::GREATER_OR_EQUAL) {
                        $predicates->add($expr->gte($where->column, $where->value));
                    } elseif ($where->operator == OperatorEnum::LIKE) {
                        if($where->not) {
                            $predicates->add($expr->notLike($where->column, $where->value));
                        } else {
                            $predicates->add($expr->like($where->column, $where->value));
                        }
                    }

                    //$queryBuilder->where($where->column, $where->operator, $where->value, $where->boolean);
                }

                $queryBuilder->where($predicates);
            }
        }
    }

    public static function setOrder(Query $query, QueryBuilder $queryBuilder)
    {
        $queryArr = $query->toArray();
        if ( ! empty($queryArr[Query::ORDER])) {
            foreach ($queryArr[Query::ORDER] as $field => $direction) {
                $queryBuilder->orderBy($field, DbHelper::encodeDirection($direction));
            }
        }
    }

    public static function setSelect(Query $query, QueryBuilder $queryBuilder)
    {
        $queryArr = $query->toArray();
        if ( ! empty($queryArr[Query::SELECT])) {
            $queryBuilder->select($queryArr[Query::SELECT]);
        }
    }

    public static function setPaginate(Query $query, QueryBuilder $queryBuilder)
    {
        $queryArr = $query->toArray();
        if ( ! empty($queryArr[Query::LIMIT])) {
            $queryBuilder->setMaxResults($queryArr[Query::LIMIT]);
        }
        if ( ! empty($queryArr[Query::OFFSET])) {
            $queryBuilder->setFirstResult($queryArr[Query::OFFSET]);
        }
    }

}