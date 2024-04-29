using System.Diagnostics;
using Coravel;
using Coravel.Scheduling.Schedule.Interfaces;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace MongoTest;

public class Program
{
    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.Host.ConfigureServices((context, services) =>
        {
            services.AddScheduler();

            services.AddHostedService<Hub>();
        });

        var app = builder.Build();

        app.MapGet("/", () => "Hello World!");

        app.Run();
        Console.WriteLine("bye bye");
    }

    internal class Hub : IHostedService
    {
        private readonly IServiceProvider _provider;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<Hub> _logger;
        private readonly IScheduler _scheduler;

        private readonly int _clientCount;
        private readonly int _writeQps;
        private readonly int _writeQpsPerClient;
        private readonly int _uidCount;
        private readonly int _uidCountPerClient;
        private readonly int _valueLength;
        private readonly byte[] _value;

        private readonly Client[] _clients;

        private volatile bool _stopped = false;

        // stats
        private int _writeCount;
        private long _writeMillis;

        private readonly ReplaceOptions _replaceOptions = new ()
        {
            IsUpsert = true,
        };

        public Hub(IServiceProvider provider, ILoggerFactory loggerFactory, IConfiguration configuration, IScheduler scheduler)
        {
            _provider = provider;
            _loggerFactory = loggerFactory;
            _logger = _loggerFactory.CreateLogger<Hub>();
            _scheduler = scheduler;

            _clientCount = configuration.GetValue<int>("ClientCount");
            _uidCount = configuration.GetValue<int>("UidCount");
            _uidCountPerClient = _uidCount / _clientCount;

            var writeIntervalSeconds = configuration.GetValue<int>("WriteIntervalSeconds");
            _writeQps = _uidCount / writeIntervalSeconds;
            _writeQpsPerClient = _writeQps / _clientCount;

            _valueLength = configuration.GetValue<int>("ValueLength");
            _value = new byte[_valueLength];
            for (var i = 0; i < _valueLength; ++i)
            {
                var v = Random.Shared.Next(256);
                _value[i] = (byte)v;
            }

            var uidBase = 1_0000_0000;
            _clients = new Client[_clientCount];
            for (var i = 0; i < _clients.Length; ++i)
            {
                _clients[i] = new Client(this, i, uidBase + _uidCountPerClient * i);
            }
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogError("Test start.");
            _scheduler.Schedule(PrintStats).EverySeconds(5);

            foreach (var client in _clients)
            {
                client.Start();
            }
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogError("Test stop.");
            return Task.CompletedTask;
        }

        private void PrintStats()
        {
            var writeCount = Interlocked.Exchange(ref _writeCount, 0);
            var writeMillis = Interlocked.Exchange(ref _writeMillis, 0);
            _logger.LogWarning("write qps:{writeQps}´Î/Ãë delay:{writeDelay}ms"
                , writeCount*1.0/5, writeCount == 0 ? 0 : writeMillis*1.0/writeCount);
        }


        private class  Client
        {
            private readonly Hub _hub;
            private readonly int _index;
            private readonly int _uidStart;
            private int _updateOffset = 0;

            private IMongoCollection<TestData> _collection;

            public Client(Hub hub, int index, int uidStart)
            {
                _hub = hub;
                _index = index;
                _uidStart = uidStart;
            }

            public void Start()
            {
                // var mongoClient = new MongoClient("mongodb://admin:admin@localhost:27017/");
                var mongoClient = new MongoClient("mongodb://admin:admin@172.18.2.101:27017/");
                var database = mongoClient.GetDatabase("test");
                _collection = database.GetCollection<TestData>("test");

                _hub._scheduler.Schedule(Tick).EverySecond();

            }

            private async void Tick()
            {
                if (_hub._stopped)
                {
                    return;
                }

                for (var i = 0; i < _hub._writeQpsPerClient; ++i)
                {
                    var offset = _updateOffset;
                    Interlocked.Increment(ref _updateOffset);

                    var uid = _uidStart + offset % _hub._uidCountPerClient;

                    for (var j = 0; j < 100; ++j)
                    {
                        var idx = Random.Shared.Next(_hub._valueLength);
                        _hub._value[idx] = (byte) Random.Shared.Next(256);
                    }

                    var sw = Stopwatch.StartNew();
                    await _collection.ReplaceOneAsync(x => x.key == uid, new TestData()
                    {
                        key = uid,
                        value = _hub._value,
                    }, _hub._replaceOptions).ConfigureAwait(false);

                    Interlocked.Increment(ref _hub._writeCount);
                    Interlocked.Add(ref _hub._writeMillis, sw.ElapsedMilliseconds);
                }
            }
        }
    }

    public class TestData
    {
        [BsonId]
        public long key;

        public byte[] value;
    }

}