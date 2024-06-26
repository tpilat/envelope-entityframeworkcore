using System.ComponentModel;
using System.Linq.Expressions;

namespace Envelope.EntityFrameworkCore.Expressions.Sorting;

internal class SortDescriptorCollectionExpressionBuilder<T>
{
	private readonly IEnumerable<SortDescriptor> sortDescriptors;
	private readonly IQueryable<T> queryable;

	public SortDescriptorCollectionExpressionBuilder(IQueryable<T> queryable, IEnumerable<SortDescriptor> sortDescriptors)
	{
		this.queryable = queryable.OrderBy(x => x);
		this.sortDescriptors = sortDescriptors;
	}

	public IQueryable<T> Sort()
	{
		var query = queryable;
		bool isFirst = true;

		foreach (var descriptor in sortDescriptors)
		{
			Type memberType = typeof(object);
			var descriptorBuilder = ExpressionBuilderFactory.MemberAccess(queryable, memberType, descriptor.Member);
			var expression = descriptorBuilder.CreateLambdaExpression();

			string methodName = "";
			if (isFirst)
			{
				methodName = descriptor.SortDirection == ListSortDirection.Ascending
					? nameof(Queryable.OrderBy)
					: nameof(Queryable.OrderByDescending);

				isFirst = false;
			}
			else
			{
				methodName = descriptor.SortDirection == ListSortDirection.Ascending
					? nameof(Queryable.ThenBy)
					: nameof(Queryable.ThenByDescending);
			}

			query = query.Provider.CreateQuery<T>(
				Expression.Call(
					typeof(Queryable),
					methodName,
					new[] { query.ElementType, expression.Body.Type },
					query.Expression,
					Expression.Quote(expression)));

		}
		return query;
	}
}
