using Microsoft.Azure.WebJobs.Host;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace IocDownsampler
{
    public static class Timer
    {
        public static async Task<T> Time<T>(Func<Task<T>> func, string message, TraceWriter log)
        {
            var stopwatch = CreateAndStartStopwatch();

            var result = await func();

            Log(message, log, stopwatch);

            return result;
        }

        public static async Task Time(Func<Task> func, string message, TraceWriter log)
        {
            var stopwatch = CreateAndStartStopwatch();

            await func();

            Log(message, log, stopwatch);
        }

        public static T TimeSync<T>(Func<T> func, string message, TraceWriter log)
        {
            var stopwatch = CreateAndStartStopwatch();

            var result = func();

            Log(message, log, stopwatch);

            return result;
        }

        private static Stopwatch CreateAndStartStopwatch()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            return stopwatch;
        }

        private static void Log(string message, TraceWriter log, Stopwatch stopwatch)
        {
            log.Info($"{message} took {stopwatch.ElapsedMilliseconds} ms");
        }
    }
}