namespace MultiDBHelper
{
    public sealed class UnitOfWork : IUnitOfWork
    {
        private readonly DbSession _dbSession;

        public UnitOfWork(DbSession session)
        {
            _dbSession = session;
        }

        public void BeginTransaction()
        {
            _dbSession.Transaction = _dbSession.Connection.BeginTransaction();
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
