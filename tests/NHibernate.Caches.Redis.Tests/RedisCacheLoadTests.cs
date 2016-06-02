using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.Cache;
using NHibernate.Linq;
using Xunit;

namespace NHibernate.Caches.Redis.Tests
{
	public class RedisCacheLoadTests : RedisCacheTests
	{
		private readonly RedisCacheProviderOptions options;

		public RedisCacheLoadTests()
		{
			options = CreateTestProviderOptions();
		}

		[Fact]
		void Clear_preformance_with_index()
		{
			DisableLogging();
			var loadRegion = new RedisCache(new RedisCacheConfiguration("region1") { Expiration = TimeSpan.FromSeconds(1) }, ConnectionMultiplexer, options);
			var anotherRegion = new RedisCache(new RedisCacheConfiguration("region2") { Expiration = TimeSpan.FromSeconds(60) }, ConnectionMultiplexer, options);
			Clear_preformance_internal(loadRegion, anotherRegion);
		}


		[Fact]
		void Clear_preformance_without_index()
		{
			DisableLogging();
			var loadRegion = new RedisCache(new RedisCacheConfiguration("region1") { Expiration = TimeSpan.FromSeconds(5), DisableIndexSetOnKeys = true }, ConnectionMultiplexer, options);
			var anotherRegion = new RedisCache(new RedisCacheConfiguration("region2") { Expiration = TimeSpan.FromSeconds(60), DisableIndexSetOnKeys = true }, ConnectionMultiplexer, options);
			Clear_preformance_internal(loadRegion, anotherRegion);
		}

		private void Clear_preformance_internal(RedisCache loadRegion, RedisCache anotherRegion)
		{
			//fill cache with sample data
			var s = Stopwatch.StartNew();
			Enumerable.Range(1, 500000).ForEach(x =>
			{
				var guid = Guid.NewGuid().ToString("N") + Guid.NewGuid().ToString("N");
				if (x == 480000) Thread.Sleep(10000);//wait for most to expire, then add some more
				loadRegion.Put(guid, guid);
			});
			s.Stop();
			Console.WriteLine($"Setup took {s.ElapsedMilliseconds}");
			//wait a bit for expiry (and redis cleanup)

			//get cache state - should be similar on both runs
			var firstMaster = ConnectionMultiplexer.GetEndPoints().Select(x => ConnectionMultiplexer.GetServer(x)).Where(x => x.IsConnected).OrderBy(x => x.IsSlave).First();
			var info = firstMaster.Info("Keyspace");
			Console.WriteLine($"Test db keyspace: {info.FirstOrDefault(x => x.Key == "Keyspace")?.FirstOrDefault(x => x.Key == "db15")}");
			//start 5 paralel tasks - 2 read-writes (simulates cache miss) on loadtest region, 2 read-writes on another region and clear on loadtest region
			var taskList = new List<Task>
			{
				new Task(() => ReadWrite(loadRegion)),
				new Task(() => ReadWrite(loadRegion)),
				new Task(() => ReadWrite(anotherRegion)),
				new Task(() => ReadWrite(anotherRegion)),
				new Task(() => ClearDb(loadRegion)),
			};
			taskList.ForEach(x => x.Start());
			Task.WaitAll(taskList.ToArray());
		}

		private static void ReadWrite(ICache cache)
		{
			Console.WriteLine("Read-Write started");//all these should be prior to clear
			try
			{
				for (var i = 0; i < 500; i++)//run for ~ 5 seconds (+delays from code)
				{
					var guid = Guid.NewGuid().ToString();
					var sw = Stopwatch.StartNew();
					cache.Get(guid);
					cache.Put(guid, guid);
					sw.Stop();
					//Console.WriteLine(sw.ElapsedMilliseconds);
					if (sw.ElapsedMilliseconds > 5) Console.WriteLine($"Read-Write took {sw.ElapsedMilliseconds}ms on {cache.RegionName}");
					Thread.Sleep(10);
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Read-Write crashed with ex: {ex}");
			}
			Console.WriteLine("Read-Write ended");//all these should be after clear
		}

		private static void ClearDb(ICache cache)
		{
			//wait a bit for other tasks to start
			Thread.Sleep(1000);
			Console.WriteLine("Clear starting");
			var sw = Stopwatch.StartNew();
			cache.Clear();
			sw.Stop();
			Console.WriteLine("Clear finished, took " + sw.ElapsedMilliseconds);
		}


	}
}
