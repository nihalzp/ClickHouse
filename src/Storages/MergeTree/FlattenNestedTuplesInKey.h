#pragma once

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/IAST.h>

#include <unordered_set>

namespace DB
{
namespace MergeTreeKeyUtils
{

inline bool isTupleOperator(const ASTPtr & ast)
{
    if (!ast)
        return true;

    if (const auto * elem = ast->as<ASTStorageOrderByElement>())
        return isTupleOperator(elem->children.front());

    if (const auto * func = ast->as<ASTFunction>(); func && func->name == "tuple")
        return func->is_operator;

    return true;
}

inline void flattenTupleKeyExpressions(const ASTPtr & ast, ASTs & out)
{
    if (!ast)
        return;

    if (const auto * func = ast->as<ASTFunction>(); func && func->name == "tuple" && func->arguments)
    {
        for (const auto & child : func->arguments->children)
            flattenTupleKeyExpressions(child, out);
        return;
    }

    out.push_back(ast->clone());
}

struct FlattenedSortKeyElement
{
    ASTPtr expression;
    int direction = 1; /// 1 for ASC, -1 for DESC
};

struct SortKeyElementContext
{
    int direction = 1;
};

struct IASTHashHasher
{
    size_t operator()(const IASTHash & hash) const noexcept { return static_cast<size_t>(CityHash_v1_0_2::Hash128to64(hash)); }
};

inline void applySortDirection(SortKeyElementContext & context, int direction)
{
    if (direction < 0)
        context.direction = -context.direction;
}

inline void flattenTupleSortingKey(
    const ASTPtr & ast, SortKeyElementContext context, std::vector<FlattenedSortKeyElement> & out, bool & has_sort_directions)
{
    if (!ast)
        return;

    /// We preserve direction semantics while flattening tuples by carrying the effective direction in `context`
    /// (to handle cases like `ORDER BY (a, (b, c)) DESC`, where DESC applies to all nested elements).
    if (const auto * elem = ast->as<ASTStorageOrderByElement>())
    {
        has_sort_directions = true;
        applySortDirection(context, elem->direction);
        flattenTupleSortingKey(elem->children.front(), context, out, has_sort_directions);
        return;
    }

    if (const auto * func = ast->as<ASTFunction>(); func && func->name == "tuple" && func->arguments)
    {
        for (const auto & child : func->arguments->children)
            flattenTupleSortingKey(child, context, out, has_sort_directions);
        return;
    }

    if (context.direction < 0)
        has_sort_directions = true;

    FlattenedSortKeyElement element;
    element.expression = ast->clone();
    element.direction = context.direction;

    out.push_back(std::move(element));
}

inline ASTs deduplicateKeyExpressions(ASTs elements)
{
    /// Key expressions can be repeated in deeply nested tuples; removing duplicates makes the stored key
    /// deterministic and avoids wasting key columns
    ASTs result;
    result.reserve(elements.size());

    std::unordered_set<IASTHash, IASTHashHasher> seen;
    seen.reserve(elements.size());

    for (auto & element : elements)
    {
        if (seen.emplace(element->getTreeHash(/*ignore_aliases=*/true)).second)
            result.push_back(std::move(element));
    }

    return result;
}

inline std::vector<FlattenedSortKeyElement> deduplicateSortKeyExpressions(std::vector<FlattenedSortKeyElement> elements)
{
    /// ORDER BY carries direction metadata, but duplicates are defined by the expression itself.
    /// We intentionally keep the first occurrence (including its direction) and drop subsequent duplicates
    /// because it does not change semantics of the sorting key.
    std::vector<FlattenedSortKeyElement> result;
    result.reserve(elements.size());

    std::unordered_set<IASTHash, IASTHashHasher> seen;
    seen.reserve(elements.size());

    for (auto & element : elements)
    {
        if (seen.emplace(element.expression->getTreeHash(/*ignore_aliases=*/true)).second)
            result.push_back(std::move(element));
    }

    return result;
}

inline ASTPtr buildTupleAST(ASTs elements, bool as_operator)
{
    if (elements.size() == 1)
        return elements.front();

    auto tuple_function = make_intrusive<ASTFunction>();
    tuple_function->name = "tuple";
    tuple_function->is_operator = as_operator;
    tuple_function->arguments = make_intrusive<ASTExpressionList>();
    tuple_function->children.push_back(tuple_function->arguments);
    tuple_function->arguments->children = std::move(elements);
    return tuple_function;
}

inline ASTPtr buildSortingKeyAST(std::vector<FlattenedSortKeyElement> elements, bool as_operator, bool has_sort_directions)
{
    /// When there are no explicit directions, we avoid wrapping every element into ASTStorageOrderByElement
    /// because that would change the stored AST shape and SHOW CREATE formatting without adding information.
    if (elements.size() == 1)
    {
        const auto & element = elements.front();

        if (element.direction < 0)
        {
            auto storage_elem = make_intrusive<ASTStorageOrderByElement>();
            storage_elem->children.push_back(element.expression);
            storage_elem->direction = element.direction;
            return storage_elem;
        }

        return element.expression;
    }

    ASTs args;
    args.reserve(elements.size());

    if (has_sort_directions)
    {
        for (auto & element : elements)
        {
            auto storage_elem = make_intrusive<ASTStorageOrderByElement>();
            storage_elem->children.push_back(std::move(element.expression));
            storage_elem->direction = element.direction;
            args.push_back(std::move(storage_elem));
        }
    }
    else
    {
        for (auto & element : elements)
            args.push_back(std::move(element.expression));
    }

    return buildTupleAST(std::move(args), as_operator);
}

inline ASTPtr flattenAndDeduplicateKeyAST(const ASTPtr & ast)
{
    if (!ast)
        return nullptr;

    const bool as_operator = isTupleOperator(ast);

    ASTs flattened;
    flattenTupleKeyExpressions(ast, flattened);
    flattened = deduplicateKeyExpressions(std::move(flattened));

    return buildTupleAST(std::move(flattened), as_operator);
}

inline ASTPtr flattenAndDeduplicateSortingKeyAST(const ASTPtr & ast)
{
    /// ORDER BY (sorting key) may include directions (ASC/DESC) and those directions can be nested or implied
    /// by an outer DESC, so flattening must carry and re-materialize direction metadata in the storage AST.
    if (!ast)
        return nullptr;

    const bool as_operator = isTupleOperator(ast);

    std::vector<FlattenedSortKeyElement> flattened;
    bool has_sort_directions = false;
    flattenTupleSortingKey(ast, SortKeyElementContext{}, flattened, has_sort_directions);
    flattened = deduplicateSortKeyExpressions(std::move(flattened));

    return buildSortingKeyAST(std::move(flattened), as_operator, has_sort_directions);
}

}
}
