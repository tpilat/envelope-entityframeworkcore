﻿using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using Envelope.EntityFrameworkCore.Internal;
using Envelope.Model;
using Envelope.Services;
using Envelope.Transactions;
using System.Data.Common;

namespace Envelope.EntityFrameworkCore;

public abstract class DbContextRepository<TEntity, TIdentity> : RepositoryBase<TEntity, TIdentity>, IRepository<TEntity>
	where TEntity : IEntity
	where TIdentity : struct
{
	protected IDbContextCache DbContextCache { get; private set; }

	public DbContextRepository(IDbContextCache dbContextCache, ILogger logger)
		: base(logger)
	{
		DbContextCache = dbContextCache ?? throw new ArgumentNullException(nameof(dbContextCache));
	}

	protected TContext GetOrCreateDbContextWithoutTransaction<TContext>(DbConnection? externalDbConnection = null, string? connectionString = null)
		where TContext : IDbContext
		=> DbContextCache.GetOrCreateIDbContextWithoutTransaction<TContext>(externalDbConnection, connectionString, null, null);

	protected TContext GetOrCreateDbContextWithExistingTransaction<TContext>(IDbContextTransaction dbContextTransaction)
		where TContext : IDbContext
		=> DbContextCache.GetOrCreateIDbContextWithExistingTransaction<TContext>(dbContextTransaction, null, null);

	protected TContext GetOrCreateDbContextWithNewTransaction<TContext>(ITransactionContext? transactionContext = null)
		where TContext : IDbContext
	{
		if (transactionContext is TransactionDbContext<TIdentity> transactionDbContext)
			return transactionDbContext.GetOrCreateIDbContextWithNewTransaction<TContext>(null, null, null, null, null);
		else
			return DbContextCache.GetOrCreateIDbContextWithNewTransaction<TContext>(transactionContext, null, null, null, null, null);
	}
}