<?php

declare(strict_types = 1);

namespace LamiasDataGateway;

use \DateTime;

use Laminas\Db\Adapter\Adapter;

use Laminas\Db\Sql\{
    Delete,
    Insert,
    Join,
    Predicate\Predicate,
    Predicate\PredicateSet,
    Select,
    Sql,
    Update,
    Where,
};

use Collection\Collection;

use App\Abstraction\ModelAbstraction;

class DataGateway
{
    private ?Adapter $adapter = null;

    public function __construct(Adapter $adapter)
    {
        $this->adapter = $adapter;
    }

    public function get(string $modelClass, array $joins = [], Predicate ...$predicates): Collection
    {
        $sql = new Sql($this->adapter);
        $query = $sql->select();
        $query->from($this->model2Entity($modelClass));

        $resultSet = new Collection('object', $modelClass);

        foreach ($joins as $join) {
            assert($join['on'] instanceof Predicate);
            assert(is_array($join['cols']));

            $query->join(
                $this->model2Entity($join['table']),
                $join['on'],
                $join['cols'],
                $join['type'],
            );
        }

        foreach ($predicates as $predicate) {
            $query->where($predicate);
        }

        $stmt = $sql->prepareStatementForSqlObject($query);

        foreach ($stmt->execute() as $record) {
            $resultSet->add(new $modelClass($record));
        }

        return $resultSet;
    }

    public function persist(ModelAbstraction $model,?Predicate ...$predicates): void
    {
        $extracted = $model->extract();
        unset($extracted['extraData']);
        unset($extracted['id']);

        if (isset($extracted['id']) and $extracted['id'] === null) {
            unset($extracted['id']);
        }

        $columns = array_keys($extracted);
        $values = array_values($extracted);

        if (count($columns) !== count($values)) {
            throw new Exception('Column count and value count don\'t match.');
        }

        $sql = new Sql($this->adapter);

        if ($model->getId() === null) {
            $query = $sql->insert();
            $query->into($this->model2Entity($model::class));
            $query->columns($columns);
            $query->values($values);
        } else {
            $query = $sql->update($this->model2Entity($model::class));
            $query->set($extracted);

            foreach ($predicates as $predicate) {
                $query->where($predicate);
            }
        }

        $stmt = $sql->prepareStatementForSqlObject($query);

        try {
            $stmt->execute();
        } catch (Exception $e) {
            throw $e;
        }
    }

    public function delete(ModelAbstraction $model, bool $soft = true, Predicate ...$predicates): void
    {
        if ($soft) {
            $this->persist($model, $predicates);
        }

        $sql = new Sql($this->adapter);
        $query = $sql->delete();
        $query->from($this->model2Entity($model::class));

        foreach ($predicates as $predicate) {
            $query->where($predicate);
        }

        $stmt = $sql->prepareStatementForSqlObject($query);

        try {
            $stmt->execute();
        } catch (Exception $e) {
            throw $e;
        }
    }

    private function model2Entity(string $modelClass): string
    {
        $class = substr($modelClass, strrpos($modelClass, '\\') + 1);
        $class = preg_replace('/[A-Z]/', '_${0}', $class);
        $class = strtolower(substr($class, 1));

        return $class;
    }
}
