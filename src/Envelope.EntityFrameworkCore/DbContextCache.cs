﻿using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Envelope.Transactions;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using Envelope.Database.PostgreSql;
using Envelope.Database;

namespace Envelope.EntityFrameworkCore;

public class DbContextCache : IDbContextCache, ITransactionCache, IDisposable, IAsyncDisposable
{
	private readonly IDbContextTransactionBehaviorObserverFactory _dbContextTransactionBehaviorObserverFactory;
	private readonly ConcurrentDictionary<string, DbContext> _dbContextCache;
	private readonly ConcurrentDictionary<string, IDbContext> _idbContextCache;
	private readonly IServiceProvider _serviceProvider;

	private bool _disposed;

	public ITransactionCoordinator TransactionCoordinator { get; private set; }

	public DbContextCache(IServiceProvider serviceProvider, IDbContextTransactionBehaviorObserverFactory dbContextTransactionBehaviorObserverFactory)
	{
		_dbContextCache = new ConcurrentDictionary<string, DbContext>();
		_idbContextCache = new ConcurrentDictionary<string, IDbContext>();
		_serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
		_dbContextTransactionBehaviorObserverFactory = dbContextTransactionBehaviorObserverFactory ?? throw new ArgumentNullException(nameof(dbContextTransactionBehaviorObserverFactory));
		TransactionCoordinator = null!;
	}

	public void SetTransactionCoordinatorInternal(ITransactionCoordinator transactionCoordinator)
	{
		TransactionCoordinator = transactionCoordinator ?? throw new ArgumentNullException(nameof(transactionCoordinator));
	}

	/// <inheritdoc />
	public TContext CreateNewDbContext<TContext>(
		DbConnection? externalDbConnection = null,
		string? connectionString = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
		=> DbContextFactory.CreateNewDbContextWithoutTransaction<TContext>(
			_serviceProvider,
			externalDbConnection,
			connectionString,
			commandQueryName,
			idCommandQuery);

	/// <inheritdoc />
	public TContext CreateNewDbContextWithNewTransaction<TContext>(
		out IDbContextTransaction newDbContextTransaction,
		ITransactionCoordinator? transactionCoordinator = null,
		IsolationLevel? transactionIsolationLevel = null,
		DbConnection? externalDbConnection = null,
		string? connectionString = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
	{
		var dbContext= 
			DbContextFactory.CreateNewDbContext<TContext>(
				_serviceProvider,
				out newDbContextTransaction,
				transactionIsolationLevel,
				externalDbConnection,
				connectionString,
				commandQueryName,
				idCommandQuery);

		if (transactionCoordinator != null && newDbContextTransaction != null)
		{
			var transaction = newDbContextTransaction;
			var observer = _dbContextTransactionBehaviorObserverFactory.Create(transaction);
			transactionCoordinator.ConnectTransactionObserver(observer);
		}

		return dbContext;
	}

	/// <inheritdoc />
	public TContext CreateNewDbContextWithExistingTransaction<TContext>(
		IDbContextTransaction dbContextTransaction,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
	{
		var result = DbContextFactory.CreateNewDbContext<TContext>(
			_serviceProvider,
			dbContextTransaction,
			out var newDbContextTransaction,
			commandQueryName,
			idCommandQuery);

		if (transactionCoordinator != null && newDbContextTransaction != null)
		{
			var observer = _dbContextTransactionBehaviorObserverFactory.Create(newDbContextTransaction);
			transactionCoordinator.ConnectTransactionObserver(observer);
		}

		return result;
	}

	/// <inheritdoc />
	public TContext CreateNewDbContextWithExistingTransaction<TContext>(
		DbTransaction dbTransaction,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
	{
		var result = DbContextFactory.CreateNewDbContext<TContext>(
			_serviceProvider,
			dbTransaction,
			out var newDbContextTransaction,
			commandQueryName,
			idCommandQuery);

		if (transactionCoordinator != null && newDbContextTransaction != null)
		{
			var observer = _dbContextTransactionBehaviorObserverFactory.Create(newDbContextTransaction);
			transactionCoordinator.ConnectTransactionObserver(observer);
		}

		return result;
	}

	/// <inheritdoc />
	public TContext GetOrCreateDbContextWithoutTransaction<TContext>(
		DbConnection? externalDbConnection = null,
		string? connectionString = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
		=> GetOrCreateDbContextWithoutTransaction<TContext>(
			typeof(TContext).FullName!,
			externalDbConnection,
			connectionString,
			commandQueryName,
			idCommandQuery);

	/// <inheritdoc />
	public TContext GetOrCreateDbContextWithNewTransaction<TContext>(
		ITransactionCoordinator? transactionCoordinator = null,
		IsolationLevel? transactionIsolationLevel = null,
		DbConnection? externalDbConnection = null,
		string? connectionString = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
		=> GetOrCreateDbContextWithNewTransaction<TContext>(
			typeof(TContext).FullName!,
			transactionCoordinator,
			transactionIsolationLevel,
			externalDbConnection,
			connectionString,
			commandQueryName,
			idCommandQuery);

	/// <inheritdoc />
	public TContext GetOrCreateDbContextWithExistingTransaction<TContext>(
		IDbContextTransaction dbContextTransaction,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
		=> GetOrCreateDbContextWithExistingTransaction<TContext>(
			typeof(TContext).FullName!,
			dbContextTransaction,
			transactionCoordinator,
			commandQueryName,
			idCommandQuery);

	/// <inheritdoc />
	public TContext GetOrCreateDbContextWithExistingTransaction<TContext>(
		DbTransaction dbTransaction,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
		=> GetOrCreateDbContextWithExistingTransaction<TContext>(
			typeof(TContext).FullName!,
			dbTransaction,
			transactionCoordinator,
			commandQueryName,
			idCommandQuery);

	/// <inheritdoc />
	public TContext GetOrCreateDbContextWithExistingTransaction<TContext>(
		IDbTransactionFactory dbTransactionFactory,
		string connectionId,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
		=> GetOrCreateDbContextWithExistingTransaction<TContext>(
			typeof(TContext).FullName!,
			dbTransactionFactory,
			connectionId,
			transactionCoordinator,
			commandQueryName,
			idCommandQuery);

	/// <inheritdoc />
	public Task<TContext> GetOrCreateDbContextWithExistingTransactionAsync<TContext>(
		IDbTransactionFactory dbTransactionFactory,
		string connectionId,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null,
		CancellationToken cancellationToken = default)
		where TContext : DbContext
		=> GetOrCreateDbContextWithExistingTransactionAsync<TContext>(
			typeof(TContext).FullName!,
			dbTransactionFactory,
			connectionId,
			transactionCoordinator,
			commandQueryName,
			idCommandQuery,
			cancellationToken);

	/// <inheritdoc />
	public TContext GetOrCreateDbContextWithoutTransaction<TContext>(
		string key,
		DbConnection? externalDbConnection = null,
		string? connectionString = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
	{
		if (string.IsNullOrWhiteSpace(key))
			throw new ArgumentNullException(nameof(key));

		var result = _dbContextCache.GetOrAdd(
			key,
			dbContextType =>
				DbContextFactory.CreateNewDbContextWithoutTransaction<TContext>(
					_serviceProvider,
					externalDbConnection,
					connectionString,
					commandQueryName,
					idCommandQuery));

		return (TContext)result;
	}

	/// <inheritdoc />
	public TContext GetOrCreateDbContextWithNewTransaction<TContext>(
		string key,
		ITransactionCoordinator? transactionCoordinator = null,
		IsolationLevel? transactionIsolationLevel = null,
		DbConnection? externalDbConnection = null,
		string? connectionString = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
	{
		if (string.IsNullOrWhiteSpace(key))
			throw new ArgumentNullException(nameof(key));

		var result = _dbContextCache.GetOrAdd(key, (Func<string, DbContext>)((dbContextType)
			=>
			{
				var dbContext =
					DbContextFactory.CreateNewDbContext<TContext>(
						_serviceProvider,
						out var newDbContextTransaction,
						transactionIsolationLevel,
						externalDbConnection,
						connectionString,
						commandQueryName,
						idCommandQuery);

				if (transactionCoordinator != null && newDbContextTransaction != null)
				{
					var observer = _dbContextTransactionBehaviorObserverFactory.Create(newDbContextTransaction);
					transactionCoordinator.ConnectTransactionObserver(observer);
				}

				return dbContext;
		}));

		return (TContext)result;
	}

	/// <inheritdoc />
	public TContext GetOrCreateDbContextWithExistingTransaction<TContext>(
		string key,
		IDbContextTransaction dbContextTransaction,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
	{
		if (string.IsNullOrWhiteSpace(key))
			throw new ArgumentNullException(nameof(key));

		if (dbContextTransaction == null)
			throw new ArgumentNullException(nameof(dbContextTransaction));

		var result = _dbContextCache.GetOrAdd(key, (dbContextType)
			=>
		{
			var dbContext = DbContextFactory.CreateNewDbContext<TContext>(
					_serviceProvider,
					dbContextTransaction,
					out var newDbContextTransaction,
					commandQueryName,
					idCommandQuery);

			if (transactionCoordinator != null && newDbContextTransaction != null)
			{
				var observer = _dbContextTransactionBehaviorObserverFactory.Create(newDbContextTransaction);
				transactionCoordinator.ConnectTransactionObserver(observer);
			}

			return dbContext;
		});

		return (TContext)result;
	}

	/// <inheritdoc />
	public TContext GetOrCreateDbContextWithExistingTransaction<TContext>(
		string key,
		DbTransaction dbTransaction,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
	{
		if (string.IsNullOrWhiteSpace(key))
			throw new ArgumentNullException(nameof(key));

		if (dbTransaction == null)
			throw new ArgumentNullException(nameof(dbTransaction));

		var result = _dbContextCache.GetOrAdd(key, (dbContextType)
			=>
		{
			var dbContext = DbContextFactory.CreateNewDbContext<TContext>(
					_serviceProvider,
					dbTransaction,
					out var newDbContextTransaction,
					commandQueryName,
					idCommandQuery);

			if (transactionCoordinator != null && newDbContextTransaction != null)
			{
				var observer = _dbContextTransactionBehaviorObserverFactory.Create(newDbContextTransaction);
				transactionCoordinator.ConnectTransactionObserver(observer);
			}

			return dbContext;
		});

		return (TContext)result;
	}

	/// <inheritdoc />
	public TContext GetOrCreateDbContextWithExistingTransaction<TContext>(
		string key,
		IDbTransactionFactory dbTransactionFactory,
		string connectionId,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
	{
		if (string.IsNullOrWhiteSpace(key))
			throw new ArgumentNullException(nameof(key));

		if (dbTransactionFactory == null)
			throw new ArgumentNullException(nameof(dbTransactionFactory));

		var result = _dbContextCache.GetOrAdd(
			key,
			dbContextType =>
			{
				var dbContext = DbContextFactory.CreateNewDbContext<TContext>(
					_serviceProvider,
					connectionId,
					dbTransactionFactory,
					out var newDbContextTransaction,
					commandQueryName,
					idCommandQuery);

				if (transactionCoordinator != null && newDbContextTransaction != null)
				{
					var observer = _dbContextTransactionBehaviorObserverFactory.Create(newDbContextTransaction);
					transactionCoordinator.ConnectTransactionObserver(observer);

					var tfObserver = new DbTransactionFactoryBehaviorObserver(dbTransactionFactory);
					transactionCoordinator.ConnectTransactionObserver(tfObserver);
				}

				return dbContext;
			});

		return (TContext)result;
	}

	/// <inheritdoc />
	public async Task<TContext> GetOrCreateDbContextWithExistingTransactionAsync<TContext>(
		string key,
		IDbTransactionFactory dbTransactionFactory,
		string connectionId,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null,
		CancellationToken cancellationToken = default)
		where TContext : DbContext
	{
		if (string.IsNullOrWhiteSpace(key))
			throw new ArgumentNullException(nameof(key));

		if (dbTransactionFactory == null)
			throw new ArgumentNullException(nameof(dbTransactionFactory));

		if (_dbContextCache.TryGetValue(key, out var result))
			return (TContext)result;

		var (dbContext, newDbContextTransaction) =
			await DbContextFactory.CreateNewDbContextAsync<TContext>(
				_serviceProvider,
				connectionId,
				dbTransactionFactory,
				commandQueryName,
				idCommandQuery,
				cancellationToken);

		result = _dbContextCache.GetOrAdd(
			key,
			dbContextType =>
			{
				if (transactionCoordinator != null && newDbContextTransaction != null)
				{
					var observer = _dbContextTransactionBehaviorObserverFactory.Create(newDbContextTransaction);
					transactionCoordinator.ConnectTransactionObserver(observer);

					var tfObserver = new DbTransactionFactoryBehaviorObserver(dbTransactionFactory);
					transactionCoordinator.ConnectTransactionObserver(tfObserver);
				}

				return dbContext;
			});

		return (TContext)result;
	}

	public IDbContextTransaction? GetDbContextTransaction<TContext>()
		where TContext : DbContext
		=> GetDbContextTransaction(typeof(TContext).FullName!);

	public IDbContextTransaction? GetDbContextTransaction(string key)
	{
		if (_dbContextCache.TryGetValue(key, out DbContext? dbContext))
			return dbContext.Database.CurrentTransaction;

		return null;
	}

	/// <inheritdoc />
	public TContext CreateNewIDbContext<TContext>(
		string connectionId,
		DbConnection? externalDbConnection = null,
		string? connectionString = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
		=> DbContextFactory.CreateNewIDbContextWithoutTransaction<TContext>(
			connectionId,
			_serviceProvider,
			externalDbConnection,
			connectionString,
			commandQueryName,
			idCommandQuery);

	/// <inheritdoc />
	public TContext CreateNewIDbContextWithNewTransaction<TContext>(
		string connectionId,
		out IDbContextTransaction newDbContextTransaction,
		ITransactionCoordinator? transactionCoordinator = null,
		IsolationLevel? transactionIsolationLevel = null,
		DbConnection? externalDbConnection = null,
		string? connectionString = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
	{
		var dbContext =
			DbContextFactory.CreateNewIDbContext<TContext>(
				connectionId,
				_serviceProvider,
				out newDbContextTransaction,
				transactionIsolationLevel,
				externalDbConnection,
				connectionString,
				commandQueryName,
				idCommandQuery);

		if (transactionCoordinator != null && newDbContextTransaction != null)
		{
			var transaction = newDbContextTransaction;
			var observer = _dbContextTransactionBehaviorObserverFactory.Create(transaction);
			transactionCoordinator.ConnectTransactionObserver(observer);
		}

		return dbContext;
	}

	/// <inheritdoc />
	public TContext CreateNewIDbContextWithExistingTransaction<TContext>(
		string connectionId,
		IDbContextTransaction dbContextTransaction,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
	{
		var result = DbContextFactory.CreateNewIDbContext<TContext>(
			connectionId,
			_serviceProvider,
			dbContextTransaction,
			out var newDbContextTransaction,
			commandQueryName,
			idCommandQuery);

		if (transactionCoordinator != null && newDbContextTransaction != null)
		{
			var observer = _dbContextTransactionBehaviorObserverFactory.Create(newDbContextTransaction!);
			transactionCoordinator.ConnectTransactionObserver(observer);
		}

		return result;
	}

	/// <inheritdoc />
	public TContext CreateNewIDbContextWithExistingTransaction<TContext>(
		string connectionId,
		DbTransaction dbTransaction,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
	{
		var result = DbContextFactory.CreateNewIDbContext<TContext>(
			connectionId,
			_serviceProvider,
			dbTransaction,
			out var newDbContextTransaction,
			commandQueryName,
			idCommandQuery);

		if (transactionCoordinator != null && newDbContextTransaction != null)
		{
			var observer = _dbContextTransactionBehaviorObserverFactory.Create(newDbContextTransaction!);
			transactionCoordinator.ConnectTransactionObserver(observer);
		}

		return result;
	}

	/// <inheritdoc />
	public TContext GetOrCreateIDbContextWithoutTransaction<TContext>(
		string connectionId,
		DbConnection? externalDbConnection = null,
		string? connectionString = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
		=> GetOrCreateIDbContextWithoutTransaction<TContext>(
			typeof(TContext).FullName!,
			connectionId,
			externalDbConnection,
			connectionString,
			commandQueryName,
			idCommandQuery);

	/// <inheritdoc />
	public TContext GetOrCreateIDbContextWithNewTransaction<TContext>(
		string connectionId,
		ITransactionCoordinator? transactionCoordinator = null,
		IsolationLevel? transactionIsolationLevel = null,
		DbConnection? externalDbConnection = null,
		string? connectionString = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
		=> GetOrCreateIDbContextWithNewTransaction<TContext>(
			typeof(TContext).FullName!,
			connectionId,
			transactionCoordinator,
			transactionIsolationLevel,
			externalDbConnection,
			connectionString,
			commandQueryName,
			idCommandQuery);

	/// <inheritdoc />
	public TContext GetOrCreateIDbContextWithExistingTransaction<TContext>(
		string connectionId,
		IDbContextTransaction dbContextTransaction,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
		=> GetOrCreateIDbContextWithExistingTransaction<TContext>(
			typeof(TContext).FullName!,
			connectionId,
			dbContextTransaction,
			transactionCoordinator,
			commandQueryName,
			idCommandQuery);

	/// <inheritdoc />
	public TContext GetOrCreateIDbContextWithExistingTransaction<TContext>(
		string connectionId,
		DbTransaction dbTransaction,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
		=> GetOrCreateIDbContextWithExistingTransaction<TContext>(
			typeof(TContext).FullName!,
			connectionId,
			dbTransaction,
			transactionCoordinator,
			commandQueryName,
			idCommandQuery);

	/// <inheritdoc />
	public TContext GetOrCreateIDbContextWithExistingTransaction<TContext>(
		string connectionId,
		IDbTransactionFactory dbTransactionFactory,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
		=> GetOrCreateIDbContextWithExistingTransaction<TContext>(
			typeof(TContext).FullName!,
			connectionId,
			dbTransactionFactory,
			transactionCoordinator,
			commandQueryName,
			idCommandQuery);

	/// <inheritdoc />
	public Task<TContext> GetOrCreateIDbContextWithExistingTransactionAsync<TContext>(
		string connectionId,
		IDbTransactionFactory dbTransactionFactory,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null,
		CancellationToken cancellationToken = default)
		where TContext : IDbContext
		=> GetOrCreateIDbContextWithExistingTransactionAsync<TContext>(
			typeof(TContext).FullName!,
			connectionId,
			dbTransactionFactory,
			transactionCoordinator,
			commandQueryName,
			idCommandQuery,
			cancellationToken);

	/// <inheritdoc />
	public TContext GetOrCreateIDbContextWithoutTransaction<TContext>(
		string key,
		string connectionId,
		DbConnection? externalDbConnection = null,
		string? connectionString = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
	{
		if (string.IsNullOrWhiteSpace(key))
			throw new ArgumentNullException(nameof(key));

		var result = _idbContextCache.GetOrAdd(
			key,
			dbContextType =>
				DbContextFactory.CreateNewIDbContextWithoutTransaction<TContext>(
					connectionId,
					_serviceProvider,
					externalDbConnection,
					connectionString,
					commandQueryName,
					idCommandQuery));

		return (TContext)result;
	}

	/// <inheritdoc />
	public TContext GetOrCreateIDbContextWithNewTransaction<TContext>(
		string key,
		string connectionId,
		ITransactionCoordinator? transactionCoordinator = null,
		IsolationLevel? transactionIsolationLevel = null,
		DbConnection? externalDbConnection = null,
		string? connectionString = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
	{
		if (string.IsNullOrWhiteSpace(key))
			throw new ArgumentNullException(nameof(key));

		var result = _idbContextCache.GetOrAdd(key, dbContextType
			=>
			{
				var dbContext =
					DbContextFactory.CreateNewIDbContext<TContext>(
						connectionId,
						_serviceProvider,
						out var newDbContextTransaction,
						transactionIsolationLevel,
						externalDbConnection,
						connectionString,
						commandQueryName,
						idCommandQuery);

				if (transactionCoordinator != null && newDbContextTransaction != null)
				{
					var observer = _dbContextTransactionBehaviorObserverFactory.Create(newDbContextTransaction);
					transactionCoordinator.ConnectTransactionObserver(observer);
				}

				return dbContext;
			});

		return (TContext)result;
	}

	/// <inheritdoc />
	public TContext GetOrCreateIDbContextWithExistingTransaction<TContext>(
		string key,
		string connectionId,
		IDbContextTransaction dbContextTransaction,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
	{
		if (string.IsNullOrWhiteSpace(key))
			throw new ArgumentNullException(nameof(key));

		if (dbContextTransaction == null)
			throw new ArgumentNullException(nameof(dbContextTransaction));

		var result = _idbContextCache.GetOrAdd(
			key,
			dbContextType =>
			{
				var dbContext = DbContextFactory.CreateNewIDbContext<TContext>(
					connectionId,
					_serviceProvider,
					dbContextTransaction,
					out var newDbContextTransaction,
					commandQueryName,
					idCommandQuery);

				if (transactionCoordinator != null && newDbContextTransaction != null)
				{
					var observer = _dbContextTransactionBehaviorObserverFactory.Create(newDbContextTransaction);
					transactionCoordinator.ConnectTransactionObserver(observer);
				}

				return dbContext;
			});

		return (TContext)result;
	}

	/// <inheritdoc />
	public TContext GetOrCreateIDbContextWithExistingTransaction<TContext>(
		string key,
		string connectionId,
		DbTransaction dbTransaction,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
	{
		if (string.IsNullOrWhiteSpace(key))
			throw new ArgumentNullException(nameof(key));

		if (dbTransaction == null)
			throw new ArgumentNullException(nameof(dbTransaction));

		var result = _idbContextCache.GetOrAdd(
			key,
			dbContextType =>
			{
				var dbContext = DbContextFactory.CreateNewIDbContext<TContext>(
					connectionId,
					_serviceProvider,
					dbTransaction,
					out var newDbContextTransaction,
					commandQueryName,
					idCommandQuery);

				if (transactionCoordinator != null && newDbContextTransaction != null)
				{
					var observer = _dbContextTransactionBehaviorObserverFactory.Create(newDbContextTransaction);
					transactionCoordinator.ConnectTransactionObserver(observer);
				}

				return dbContext;
			});

		return (TContext)result;
	}

	/// <inheritdoc />
	public TContext GetOrCreateIDbContextWithExistingTransaction<TContext>(
		string key,
		string connectionId,
		IDbTransactionFactory dbTransactionFactory,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
	{
		if (string.IsNullOrWhiteSpace(key))
			throw new ArgumentNullException(nameof(key));

		if (dbTransactionFactory == null)
			throw new ArgumentNullException(nameof(dbTransactionFactory));

		var result = _idbContextCache.GetOrAdd(
			key,
			dbContextType =>
			{
				var dbContext = DbContextFactory.CreateNewIDbContext<TContext>(
					connectionId,
					_serviceProvider,
					dbTransactionFactory,
					out var newDbContextTransaction,
					commandQueryName,
					idCommandQuery);

				if (transactionCoordinator != null && newDbContextTransaction != null)
				{
					var observer = _dbContextTransactionBehaviorObserverFactory.Create(newDbContextTransaction);
					transactionCoordinator.ConnectTransactionObserver(observer);

					var tfObserver = new DbTransactionFactoryBehaviorObserver(dbTransactionFactory);
					transactionCoordinator.ConnectTransactionObserver(tfObserver);
				}

				return dbContext;
			});

		return (TContext)result;
	}

	/// <inheritdoc />
	public async Task<TContext> GetOrCreateIDbContextWithExistingTransactionAsync<TContext>(
		string key,
		string connectionId,
		IDbTransactionFactory dbTransactionFactory,
		ITransactionCoordinator? transactionCoordinator = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null,
		CancellationToken cancellationToken = default)
		where TContext : IDbContext
	{
		if (string.IsNullOrWhiteSpace(key))
			throw new ArgumentNullException(nameof(key));

		if (dbTransactionFactory == null)
			throw new ArgumentNullException(nameof(dbTransactionFactory));

		if (_idbContextCache.TryGetValue(key, out var result))
			return (TContext)result;

		var (dbContext, newDbContextTransaction) =
			await DbContextFactory.CreateNewIDbContextAsync<TContext>(
				connectionId,
				_serviceProvider,
				dbTransactionFactory,
				commandQueryName,
				idCommandQuery,
				cancellationToken);

		result = _idbContextCache.GetOrAdd(
			key,
			dbContextType =>
			{
				if (transactionCoordinator != null && newDbContextTransaction != null)
				{
					var observer = _dbContextTransactionBehaviorObserverFactory.Create(newDbContextTransaction);
					transactionCoordinator.ConnectTransactionObserver(observer);

					var tfObserver = new DbTransactionFactoryBehaviorObserver(dbTransactionFactory);
					transactionCoordinator.ConnectTransactionObserver(tfObserver);
				}

				return dbContext;
			});

		return (TContext)result;
	}

	public IDbContextTransaction? GetIDbContextTransaction<TContext>()
		where TContext : IDbContext
		=> GetIDbContextTransaction(typeof(TContext).FullName!);

	public IDbContextTransaction? GetIDbContextTransaction(string key)
	{
		if (_dbContextCache.TryGetValue(key, out DbContext? dbContext))
			return dbContext.Database.CurrentTransaction;

		return null;
	}

#if NET6_0_OR_GREATER

	public async ValueTask DisposeAsync()
	{
		if (_disposed)
			return;

		_disposed = true;

		await DisposeAsyncCoreAsync().ConfigureAwait(false);

		Dispose(disposing: false);
		GC.SuppressFinalize(this);
	}

	protected virtual async ValueTask DisposeAsyncCoreAsync()
	{
		foreach (var cacheItem in _idbContextCache.Values)
			await cacheItem.DisposeAsync();

		_idbContextCache.Clear();

		foreach (var cacheItem in _dbContextCache.Values)
			await cacheItem.DisposeAsync();

		_dbContextCache.Clear();
	}

#endif

	protected virtual void Dispose(bool disposing)
	{
		if (_disposed)
			return;

		_disposed = true;

		if (disposing)
		{
			foreach (var cacheItem in _idbContextCache.Values)
				cacheItem.Dispose();

			_idbContextCache.Clear();

			foreach (var cacheItem in _dbContextCache.Values)
				cacheItem.Dispose();

			_dbContextCache.Clear();
		}
	}

	public void Dispose()
	{
		Dispose(disposing: true);
		GC.SuppressFinalize(this);
	}
}
