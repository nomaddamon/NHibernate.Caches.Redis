using System;
using System.Threading;
namespace NHibernate.Caches.Redis
{
    internal class RedisNamespace
    {
        private readonly string prefix;
        private readonly string setOfActiveKeysKey;
		private readonly string keyPattern;

        public RedisNamespace(string prefix)
        {
            this.prefix = prefix;
            this.setOfActiveKeysKey = prefix + ":keys";
            this.keyPattern = prefix + ":*";
        }

        public string GetSetOfActiveKeysKey()
        {
            return setOfActiveKeysKey;
        }

		public string GetKeyPattern()
		{
			return keyPattern;
		}

		public string GetKey(object key)
        {
            return prefix + ":" + key;
        }

        public string GetLockKey(object key)
        {
            return GetKey(key) + ":lock";
        }
    }
}