﻿using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using System.Data;
using System.Data.Common;

namespace Envelope.EntityFrameworkCore;

public static class DbContextFactory
{
	public static TContext CreateNewDbContext<TContext, TIdentity>(
		IServiceProvider serviceProvider,
		IDbContextTransaction existingDbContextTransaction,
		out IDbContextTransaction? newDbContextTransaction,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
		where TIdentity : struct
	{
		if (serviceProvider == null)
			throw new ArgumentNullException(nameof(serviceProvider));

		if (existingDbContextTransaction == null)
			throw new ArgumentNullException(nameof(existingDbContextTransaction));

		var dbContext = serviceProvider.GetRequiredService<TContext>();
		if (dbContext is DbContextBase<TIdentity> dbContextBase)
		{
			dbContextBase.CommandQueryName = commandQueryName;
			dbContextBase.IdCommandQuery = idCommandQuery;
			dbContextBase.SetExternalConnection(
				existingDbContextTransaction.GetDbTransaction().Connection,
				null);
		}

		return SetDbTransaction(dbContext, existingDbContextTransaction, out newDbContextTransaction, TransactionUsage.Reuse, null);
	}

	public static TContext CreateNewDbContext<TContext, TIdentity>(
		IServiceProvider serviceProvider,
		out IDbContextTransaction newDbContextTransaction,
		IsolationLevel? transactionIsolationLevel = null,
		DbConnection? externalDbConnection = null,
		string? connectionString = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
		where TIdentity : struct
	{
		if (serviceProvider == null)
			throw new ArgumentNullException(nameof(serviceProvider));

		var dbContext = serviceProvider.GetRequiredService<TContext>();
		if (dbContext is DbContextBase<TIdentity> dbContextBase)
		{
			dbContextBase.CommandQueryName = commandQueryName;
			dbContextBase.IdCommandQuery = idCommandQuery;
			dbContextBase.SetExternalConnection(externalDbConnection, connectionString);
		}

		return SetDbTransaction(dbContext, null, out newDbContextTransaction!, TransactionUsage.CreateNew, transactionIsolationLevel);
	}

	public static TContext CreateNewDbContextWithoutTransaction<TContext, TIdentity>(
		IServiceProvider serviceProvider,
		DbConnection? externalDbConnection = null,
		string? connectionString = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : DbContext
		where TIdentity : struct
	{
		if (serviceProvider == null)
			throw new ArgumentNullException(nameof(serviceProvider));

		var dbContext = serviceProvider.GetRequiredService<TContext>();
		if (dbContext is DbContextBase<TIdentity> dbContextBase)
		{
			dbContextBase.CommandQueryName = commandQueryName;
			dbContextBase.IdCommandQuery = idCommandQuery;
			dbContextBase.SetExternalConnection(externalDbConnection, connectionString);
		}

		return dbContext;
	}

	public static TContext SetDbTransaction<TContext>(
		TContext dbContext,
		IDbContextTransaction? existingDbContextTransaction,
		out IDbContextTransaction? newDbContextTransaction,
		TransactionUsage transactionUsage,
		IsolationLevel? transactionIsolationLevel)
		where TContext : DbContext
	{
		newDbContextTransaction = null;

		if (dbContext == null)
			throw new ArgumentNullException(nameof(dbContext));

		if (transactionUsage == TransactionUsage.NONE)
			return dbContext;

		if (transactionUsage == TransactionUsage.Reuse)
		{
			if (existingDbContextTransaction == null)
			{
				throw new ArgumentNullException(nameof(existingDbContextTransaction));

				//if (transactionIsolationLevel.HasValue)
				//{
				//	newDbContextTransaction = dbContext.Database.BeginTransaction(transactionIsolationLevel.Value);
				//}
				//else
				//{
				//	newDbContextTransaction = dbContext.Database.BeginTransaction();
				//}

				//return dbContext;
			}
			else
			{
				if (dbContext.Database.CurrentTransaction == null)
				{
					newDbContextTransaction = existingDbContextTransaction;
					dbContext.Database.UseTransaction(newDbContextTransaction.GetDbTransaction());
					return dbContext;
				}
				else
				{
					if (dbContext.Database.CurrentTransaction.TransactionId != existingDbContextTransaction.TransactionId)
						throw new InvalidOperationException($"DbContext already has set another transaction with id {dbContext.Database.CurrentTransaction.TransactionId}");

					return dbContext;
				}
			}
		}

		if (transactionUsage == TransactionUsage.CreateNew)
		{
			if (dbContext.Database.CurrentTransaction == null)
			{
				if (transactionIsolationLevel.HasValue)
				{
					newDbContextTransaction = dbContext.Database.BeginTransaction(transactionIsolationLevel.Value);
				}
				else
				{
					newDbContextTransaction = dbContext.Database.BeginTransaction();
				}

				return dbContext;
			}
			else
			{
				throw new InvalidOperationException($"DbContext already has set another transaction with id {dbContext.Database.CurrentTransaction.TransactionId}");
			}
		}

		return dbContext;
	}

	public static TContext CreateNewIDbContext<TContext, TIdentity>(
		IServiceProvider serviceProvider,
		IDbContextTransaction existingDbContextTransaction,
		out IDbContextTransaction? newDbContextTransaction,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
		where TIdentity : struct
	{
		if (serviceProvider == null)
			throw new ArgumentNullException(nameof(serviceProvider));

		if (existingDbContextTransaction == null)
			throw new ArgumentNullException(nameof(existingDbContextTransaction));

		var dbContext = serviceProvider.GetRequiredService<TContext>();
		if (dbContext is DbContextBase<TIdentity> dbContextBase)
		{
			dbContextBase.CommandQueryName = commandQueryName;
			dbContextBase.IdCommandQuery = idCommandQuery;
			dbContextBase.SetExternalConnection(
				existingDbContextTransaction.GetDbTransaction().Connection,
				null);
		}

		dbContext.SetDbTransaction(existingDbContextTransaction, out newDbContextTransaction, TransactionUsage.Reuse, null);

		return dbContext;
	}

	public static TContext CreateNewIDbContext<TContext, TIdentity>(
		IServiceProvider serviceProvider,
		out IDbContextTransaction newDbContextTransaction,
		IsolationLevel? transactionIsolationLevel = null,
		DbConnection? externalDbConnection = null,
		string? connectionString = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
		where TIdentity : struct
	{
		if (serviceProvider == null)
			throw new ArgumentNullException(nameof(serviceProvider));

		var dbContext = serviceProvider.GetRequiredService<TContext>();
		if (dbContext is DbContextBase<TIdentity> dbContextBase)
		{
			dbContextBase.CommandQueryName = commandQueryName;
			dbContextBase.IdCommandQuery = idCommandQuery;
			dbContextBase.SetExternalConnection(externalDbConnection, connectionString);
		}

		dbContext.SetDbTransaction(null, out newDbContextTransaction!, TransactionUsage.CreateNew, transactionIsolationLevel);

		return dbContext;
	}

	public static TContext CreateNewIDbContextWithoutTransaction<TContext, TIdentity>(
		IServiceProvider serviceProvider,
		DbConnection? externalDbConnection = null,
		string? connectionString = null,
		string? commandQueryName = null,
		Guid? idCommandQuery = null)
		where TContext : IDbContext
		where TIdentity : struct
	{
		if (serviceProvider == null)
			throw new ArgumentNullException(nameof(serviceProvider));

		var dbContext = serviceProvider.GetRequiredService<TContext>();
		if (dbContext is DbContextBase<TIdentity> dbContextBase)
		{
			dbContextBase.CommandQueryName = commandQueryName;
			dbContextBase.IdCommandQuery = idCommandQuery;
			dbContextBase.SetExternalConnection(externalDbConnection, connectionString);
		}

		return dbContext;
	}
}
