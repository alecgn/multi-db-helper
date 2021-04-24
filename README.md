# MultiDBHelper
[![Build status (multi-db-helper)](https://github.com/alecgn/multi-db-helper/workflows/build/badge.svg)](#)
[![Nuget version (MultiDBHelper)](https://img.shields.io/nuget/v/MultiDBHelper)](https://nuget.org/packages/MultiDBHelper) 
[![Nuget downloads (MultiDBHelper)](https://img.shields.io/nuget/dt/MultiDBHelper)](https://nuget.org/packages/MultiDBHelper)

A Dapper-based multi-target .NET Standard library (.NET Standard 2.0/2.1) that can be used in projects with any .NET implementation, like .NET Framework, .NET Core, Mono, Xamarin, etc., for multi-database access (MS SQL Server, Oracle, PostgreSQL, MySQL, Firebird, SQLite). Usage is the same as Dapper (see https://github.com/StackExchange/Dapper for examples), plus you can select and connect to different databases in a single class, and use Transactions (with a default or user-defined isolation level) with the UnitOfWork pattern directly from the same lib.
