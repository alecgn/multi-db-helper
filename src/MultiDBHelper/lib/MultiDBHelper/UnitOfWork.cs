using System.Data;

namespace MultiDBHelper
{
    public sealed class UnitOfWork : IUnitOfWork
    {
        private readonly DbSession _dbSession;

        public UnitOfWork(DbSession dbSession)
        {
            _dbSession = dbSession;
        }

        public void BeginTransaction(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
        {
            _dbSession.Transaction = _dbSession.Connection.BeginTransaction(isolationLevel);
        }

        public void Commit()
        {
            _dbSession.Transaction.Commit();
            Dispose();
        }

        public void Rollback()
        {
            _dbSession.Transaction.Rollback();
            Dispose();
        }

        public void Dispose() => _dbSession.Transaction?.Dispose();
    }
}
