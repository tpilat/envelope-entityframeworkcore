using Microsoft.EntityFrameworkCore.Query;
using System.Collections;
using System.Linq.Expressions;

namespace Envelope.EntityFrameworkCore;

public static class AsyncEnumerableExtensions
{
	public static IQueryable<T> AsAsyncQueryable<T>(this IEnumerable<T> enumerable)
		=> new AsyncEnumerable<T>(enumerable);
}




internal class EmptyExpressionVisitor : ExpressionVisitor
{
}



internal abstract class QueryProvider<T> : IOrderedQueryable<T>, IQueryProvider
{
	private IEnumerable<T> _enumerable;

	protected QueryProvider(Expression expression)
	{
		Expression = expression;
	}

	protected QueryProvider(IEnumerable<T> enumerable)
	{
		_enumerable = enumerable;
		Expression = enumerable.AsQueryable().Expression;
	}

	public IQueryable CreateQuery(Expression expression)
	{
		if (expression is MethodCallExpression m)
		{
			var resultType = m.Method.ReturnType; // it should be IQueryable<T>
			var tElement = resultType.GetGenericArguments().First();
			return (IQueryable)CreateInstance(tElement, expression);
		}

		return CreateQuery<T>(expression);
	}

	public IQueryable<TEntity> CreateQuery<TEntity>(Expression expression)
	{
		return (IQueryable<TEntity>)CreateInstance(typeof(TEntity), expression);
	}

	private object CreateInstance(Type tElement, Expression expression)
	{
		var queryType = GetType().GetGenericTypeDefinition().MakeGenericType(tElement);
		return Activator.CreateInstance(queryType, expression)!;
	}

	public object Execute(Expression expression)
	{
		return CompileExpressionItem<object>(expression);
	}

	public TResult Execute<TResult>(Expression expression)
	{
		return CompileExpressionItem<TResult>(expression);
	}

	IEnumerator<T> IEnumerable<T>.GetEnumerator()
	{
		_enumerable ??= CompileExpressionItem<IEnumerable<T>>(Expression);
		return _enumerable.GetEnumerator();
	}

	IEnumerator IEnumerable.GetEnumerator()
	{
		_enumerable ??= CompileExpressionItem<IEnumerable<T>>(Expression);
		return _enumerable.GetEnumerator();
	}

	public Type ElementType => typeof(T);

	public Expression Expression { get; }

	public IQueryProvider Provider => this;

	private static TResult CompileExpressionItem<TResult>(Expression expression)
	{
		var visitor = new EmptyExpressionVisitor();
		var body = visitor.Visit(expression);
		var f = Expression.Lambda<Func<TResult>>(body ?? throw new InvalidOperationException($"{nameof(body)} is null"), (IEnumerable<ParameterExpression>)null!);
		return f.Compile()();
	}
}









internal class AsyncEnumerator<T> : IAsyncEnumerator<T>
{
	private readonly IEnumerator<T> _enumerator;

	public AsyncEnumerator(IEnumerator<T> enumerator)
	{
		_enumerator = enumerator ?? throw new ArgumentNullException(nameof(enumerator));
	}

	public T Current => _enumerator.Current;

	public ValueTask DisposeAsync()
	{
		_enumerator.Dispose();
		return new ValueTask();
	}

	public ValueTask<bool> MoveNextAsync()
	{
		return new ValueTask<bool>(_enumerator.MoveNext());
	}
}





internal class AsyncEnumerable<T> : QueryProvider<T>, IAsyncEnumerable<T>, IAsyncQueryProvider
{
	public AsyncEnumerable(Expression expression) : base(expression)
	{
	}

	public AsyncEnumerable(IEnumerable<T> enumerable) : base(enumerable)
	{
	}

	public TResult ExecuteAsync<TResult>(Expression expression, CancellationToken cancellationToken)
	{
		var expectedResultType = typeof(TResult).GetGenericArguments()[0];
		var executionResult = typeof(IQueryProvider)
			.GetMethods()
			.First(method => method.Name == nameof(IQueryProvider.Execute) && method.IsGenericMethod)
			.MakeGenericMethod(expectedResultType)
			.Invoke(this, new object[] { expression });

		return (TResult)typeof(Task).GetMethod(nameof(Task.FromResult))!
			.MakeGenericMethod(expectedResultType)
			.Invoke(null, new[] { executionResult })!;
	}

	public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
	{
		return new AsyncEnumerator<T>(this.AsEnumerable().GetEnumerator());
	}
}
