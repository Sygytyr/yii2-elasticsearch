<?php
/**
 * Yii2 Elasticsearch QueryBuilder (Elasticsearch 8/9 compatible)
 * - Replaces deprecated filters/types/_uid/missing syntax
 * - Builds bool query with must/should/filter/must_not
 * - Uses terms/ids/exists/range
 *
 * Notes:
 *  - Top-level `filter` is not supported in ES8+. All filters go inside `query.bool.filter`.
 *  - `_uid` was removed; use `_id` and the `ids` query.
 *  - `missing` query deprecated; use `bool.must_not: { exists: { field: x } }`.
 *  - Types removed in ES7+; the returned payload omits `type`.
 *  - `fields` (pre-5) is different now. For old `$query->fields === []` we set `stored_fields` to []. For scripted fields use `script_fields`.
 */

namespace yii\elasticsearch;

use yii\base\BaseObject;
use yii\base\InvalidParamException;
use yii\base\NotSupportedException;
use yii\helpers\Json;

class QueryBuilder extends BaseObject
{
    /** @var Connection */
    public $db;

    public function __construct($connection, $config = [])
    {
        $this->db = $connection;
        parent::__construct($config);
    }

    /**
     * Build Elasticsearch 8/9 query body from Query object
     *
     * Expected Query fields (legacy Yii2 elastic extension compatible):
     *  - index, where, filter, query, orderBy, limit, offset, minScore, explain,
     *    fields, source, highlight, aggregations, stats, suggest, postFilter, options, timeout
     */
    public function build($query)
    {
        $body = [];

        // === Fields / _source handling ===
        // Legacy: $query->fields used to be pre-5 `fields` retrieval. ES8 uses stored_fields/docvalue_fields.
        if ($query->fields === []) {
            // request no stored fields in response
            $body['stored_fields'] = [];
        } elseif ($query->fields !== null) {
            $fields = [];
            $scriptFields = [];
            foreach ($query->fields as $key => $field) {
                if (is_int($key)) {
                    // treat as docvalue_fields request for scalar fields
                    $fields[] = $field;
                } else {
                    // script field
                    $scriptFields[$key] = $field;
                }
            }
            if (!empty($fields)) {
                $body['docvalue_fields'] = $fields; // safer than legacy 'fields'
            }
            if (!empty($scriptFields)) {
                $body['script_fields'] = $scriptFields;
            }
        }
        if ($query->source !== null) {
            $body['_source'] = $query->source;
        }

        if ($query->limit !== null && $query->limit >= 0) {
            $body['size'] = (int)$query->limit;
        }
        if (!empty($query->offset)) {
            $body['from'] = (int)$query->offset;
        }
        if (isset($query->minScore)) {
            $body['min_score'] = (float)$query->minScore;
        }
        if (isset($query->explain)) {
            $body['explain'] = (bool)$query->explain;
        }

        // === Query / Bool composition ===
        $mainQuery = empty($query->query) ? ['match_all' => (object)[]] : $query->query;

        // Build filter from where + filter
        $whereFilter = $this->buildCondition($query->where);
        $extraFilter = $this->normalizeFilter($query->filter);

        $bool = [];
        // If $mainQuery is not a pure match_all, put it under must
        if ($mainQuery !== ['match_all' => (object)[]]) {
            $bool['must'][] = $mainQuery;
        } else {
            // keep match_all only if we have neither must nor filter
            $bool['must'] = $bool['must'] ?? [];
        }

        // Merge filters into bool.filter
        $filters = [];
        if (!empty($whereFilter)) {
            $filters[] = $whereFilter;
        }
        if (!empty($extraFilter)) {
            $filters[] = $extraFilter;
        }
        if (!empty($filters)) {
            $bool['filter'] = array_values($this->flattenBoolFilters($filters));
        }

        if (empty($bool)) {
            $body['query'] = ['match_all' => (object)[]];
        } else {
            $body['query'] = ['bool' => $bool];
        }

        if (!empty($query->highlight)) {
            $body['highlight'] = $query->highlight;
        }
        if (!empty($query->aggregations)) {
            $body['aggs'] = $query->aggregations;
        }
        if (!empty($query->stats)) {
            $body['stats'] = $query->stats;
        }
        if (!empty($query->suggest)) {
            $body['suggest'] = $query->suggest;
        }
        if (!empty($query->postFilter)) {
            // post_filter is still valid for ES8
            $body['post_filter'] = $query->postFilter;
        }

        $sort = $this->buildOrderBy($query->orderBy);
        if (!empty($sort)) {
            $body['sort'] = $sort;
        }

        $options = $query->options;
        if ($query->timeout !== null) {
            // search request timeout, e.g. '2s'
            $options['timeout'] = $query->timeout;
        }

        return [
            'queryParts' => $body,
            'index'      => $query->index,
            // 'type' removed in ES7+
            'options'    => $options,
        ];
    }

    /** Build ORDER BY for ES8 */
    public function buildOrderBy($columns)
    {
        if (empty($columns)) {
            return [];
        }
        $orders = [];
        foreach ($columns as $name => $direction) {
            if (is_string($direction)) {
                $column = $direction;
                $direction = SORT_ASC;
            } else {
                $column = $name;
            }
            if ($column === '_uid') {
                // _uid deprecated; fallback to _id if users had old code
                $column = '_id';
            }

            if (is_array($direction)) {
                // extended syntax already formatted
                $orders[] = [$column => $direction];
            } else {
                $orders[] = [$column => ($direction === SORT_DESC ? 'desc' : 'asc')];
            }
        }
        return $orders;
    }

    /**
     * Build a filter query clause (ES8) from condition spec.
     * Returns an ES query clause (not top-level) suitable for bool.filter/must/must_not.
     */
    public function buildCondition($condition)
    {
        static $builders = [
            'not' => 'buildNotCondition',
            'and' => 'buildAndCondition',
            'or'  => 'buildOrCondition',
            'between' => 'buildBetweenCondition',
            'not between' => 'buildNotBetweenCondition',
            'in' => 'buildInCondition',
            'not in' => 'buildNotInCondition',
            'like' => 'buildLikeCondition',
            'not like' => 'buildLikeCondition',
            'or like' => 'buildLikeCondition',
            'or not like' => 'buildLikeCondition',
            'lt' => 'buildRangeOp', '<'  => 'buildRangeOp',
            'lte'=> 'buildRangeOp', '<=' => 'buildRangeOp',
            'gt' => 'buildRangeOp', '>'  => 'buildRangeOp',
            'gte'=> 'buildRangeOp', '>=' => 'buildRangeOp',
        ];

        if (empty($condition)) {
            return [];
        }
        if (!is_array($condition)) {
            throw new NotSupportedException('String conditions in where() are not supported for ES8 builder.');
        }
        if (isset($condition[0])) {
            $operator = strtolower($condition[0]);
            if (!isset($builders[$operator])) {
                throw new InvalidParamException('Unknown operator in query: ' . $operator);
            }
            $method = $builders[$operator];
            array_shift($condition);
            return $this->$method($operator, $condition);
        }
        return $this->buildHashCondition($condition);
    }

    private function buildHashCondition($condition)
    {
        $clauses = [];
        foreach ($condition as $attribute => $value) {
            if ($attribute === '_uid') {
                $attribute = '_id'; // migrate
            }
            if ($attribute === '_id') {
                if ($value === null) {
                    // no null _id; represent as false filter (use term on impossible id)
                    $clauses[] = ['term' => ['_id' => '__null__never__']];
                } else {
                    $ids = is_array($value) ? array_values($value) : [$value];
                    $clauses[] = ['ids' => ['values' => $ids]];
                }
                continue;
            }

            if (is_array($value)) {
                // IN condition
                $clauses[] = ['terms' => [$attribute => array_values($value)]];
            } else {
                if ($value === null) {
                    $clauses[] = ['bool' => ['must_not' => [['exists' => ['field' => $attribute]]]]];
                } else {
                    // exact term match (keyword field assumed)
                    $clauses[] = ['term' => [$attribute => $value]];
                }
            }
        }
        if (count($clauses) === 1) {
            return $clauses[0];
        }
        return ['bool' => ['must' => $clauses]];
    }

    private function buildNotCondition($operator, $operands)
    {
        if (count($operands) !== 1) {
            throw new InvalidParamException("Operator '$operator' requires exactly one operand.");
        }
        $operand = reset($operands);
        if (is_array($operand)) {
            $operand = $this->buildCondition($operand);
        }
        if (empty($operand)) {
            return [];
        }
        return ['bool' => ['must_not' => [$operand]]];
    }

    private function buildAndCondition($operator, $operands)
    {
        $parts = [];
        foreach ($operands as $operand) {
            if (is_array($operand)) {
                $operand = $this->buildCondition($operand);
            }
            if (!empty($operand)) {
                $parts[] = $operand;
            }
        }
        if (empty($parts)) {
            return [];
        }
        return ['bool' => ['must' => $parts]];
    }

    private function buildOrCondition($operator, $operands)
    {
        $parts = [];
        foreach ($operands as $operand) {
            if (is_array($operand)) {
                $operand = $this->buildCondition($operand);
            }
            if (!empty($operand)) {
                $parts[] = $operand;
            }
        }
        if (empty($parts)) {
            return [];
        }
        return ['bool' => ['should' => $parts, 'minimum_should_match' => 1]];
    }

    private function buildBetweenCondition($operator, $operands)
    {
        if (!isset($operands[0], $operands[1], $operands[2])) {
            throw new InvalidParamException("Operator '$operator' requires three operands.");
        }
        [$column, $value1, $value2] = $operands;
        if ($column === '_id' || $column === '_uid') {
            throw new NotSupportedException('Between condition is not supported for the _id field.');
        }
        return ['range' => [$column => ['gte' => $value1, 'lte' => $value2]]];
    }

    private function buildNotBetweenCondition($operator, $operands)
    {
        $clause = $this->buildBetweenCondition('between', $operands);
        return ['bool' => ['must_not' => [$clause]]];
    }

    private function buildInCondition($operator, $operands)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new InvalidParamException("Operator '$operator' requires two operands.");
        }
        [$column, $values] = $operands;
        if (is_array($column)) {
            if (count($column) > 1) {
                throw new NotSupportedException('Composite IN is not supported by Elasticsearch.');
            }
            $column = reset($column);
        }
        $values = (array)$values;

        if ($column === '_uid') $column = '_id';
        if ($column === '_id') {
            if (empty($values)) {
                // false condition
                return ['term' => ['_id' => '__empty__never__']];
            }
            return ['ids' => ['values' => array_values($values)]];
        }

        if (empty($values)) {
            // no values => false condition
            return ['term' => [$column => '__empty__never__']];
        }
        return ['terms' => [$column => array_values($values)]];
    }

    private function buildNotInCondition($operator, $operands)
    {
        $clause = $this->buildInCondition('in', $operands);
        return ['bool' => ['must_not' => [$clause]]];
    }

    private function buildRangeOp($operator, $operands)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new InvalidParamException("Operator '$operator' requires two operands.");
        }
        [$column, $value] = $operands;
        if ($column === '_uid') $column = '_id';

        $map = [
            'gte' => 'gte', '>=' => 'gte',
            'lte' => 'lte', '<=' => 'lte',
            'gt'  => 'gt',  '>'  => 'gt',
            'lt'  => 'lt',  '<'  => 'lt',
        ];
        if (!isset($map[$operator])) {
            throw new InvalidParamException("Operator '$operator' is not implemented.");
        }
        return ['range' => [$column => [$map[$operator] => $value]]];
    }

    protected function buildLikeCondition($operator, $operands)
    {
        // ES does not support SQL LIKE directly. Provide sane defaults: wildcard for simple cases.
        // ['like', 'field', 'foo'] => { "wildcard": { "field": "*foo*" } }
        if (!isset($operands[0], $operands[1])) {
            throw new InvalidParamException("Operator '$operator' requires two operands.");
        }
        [$column, $value] = $operands;
        if ($value === null || $value === '') {
            return [];
        }
        // escape * and ? minimally
        $value = str_replace(['*','?'], ['\\*','\\?'], (string)$value);
        $pattern = '*' . $value . '*';
        return ['wildcard' => [$column => $pattern]];
    }

    /**
     * Normalize `$query->filter` (legacy string/array) to ES clause.
     */
    private function normalizeFilter($filter)
    {
        if ($filter === null || $filter === []) {
            return [];
        }
        if (is_string($filter)) {
            // try to decode JSON string; fall back to raw match_all if invalid
            $decoded = null;
            try {
                $decoded = Json::decode($filter, true);
            } catch (\Throwable $e) {
                $decoded = null;
            }
            return is_array($decoded) ? $decoded : [];
        }
        if (is_array($filter)) {
            return $filter;
        }
        return [];
    }

    /**
     * Flattens nested bool.filter arrays while preserving must/should/not semantics.
     */
    private function flattenBoolFilters(array $filters)
    {
        $flat = [];
        foreach ($filters as $f) {
            if (!is_array($f) || !isset($f['bool'])) {
                $flat[] = $f;
                continue;
            }
            // if it's a pure bool with only filter, merge its filter; else keep as-is
            $b = $f['bool'];
            $hasOnlyFilter = (isset($b['filter']) && count($b) === 1);
            if ($hasOnlyFilter) {
                foreach ((array)$b['filter'] as $inner) {
                    $flat[] = $inner;
                }
            } else {
                $flat[] = $f;
            }
        }
        return $flat;
    }
}
