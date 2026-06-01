using System.Diagnostics;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Validators;

namespace Slon.Benchmark;

public class Program
{
	public static void Main(string[] args)
	{
#if DEBUG
var config = new DebugInProcessConfig();
#else
		var config = DefaultConfig.Instance;
#endif
		BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
	}
}

public class CpuDiagnoserAttribute : Attribute, IConfigSource
{
	public IConfig Config { get; }

	public CpuDiagnoserAttribute()
	{
		Config = ManualConfig.CreateEmpty().AddDiagnoser(new CpuDiagnoser());
	}
}

public class CpuDiagnoser : IDiagnoser
{
	readonly Process _proc;

	public CpuDiagnoser()
	{
		_proc = Process.GetCurrentProcess();
	}

	public IEnumerable<string> Ids => new[] { "CPU" };

	public IEnumerable<IExporter> Exporters => Array.Empty<IExporter>();

	public IEnumerable<IAnalyser> Analysers => Array.Empty<IAnalyser>();

	public void DisplayResults(ILogger logger)
	{
	}

	public RunMode GetRunMode(BenchmarkCase benchmarkCase)
	{
		return RunMode.NoOverhead;
	}

	long userStart, userEnd;
	long privStart, privEnd;

	public void Handle(HostSignal signal, DiagnoserActionParameters parameters)
	{
		if(signal == HostSignal.BeforeActualRun)
		{
			userStart = _proc.UserProcessorTime.Ticks;
			privStart = _proc.PrivilegedProcessorTime.Ticks;
		}
		if(signal == HostSignal.AfterActualRun)
		{
			userEnd = _proc.UserProcessorTime.Ticks;
			privEnd = _proc.PrivilegedProcessorTime.Ticks;
		}
	}

	public IEnumerable<Metric> ProcessResults(DiagnoserResults results)
	{
		yield return new Metric(CpuUserMetricDescriptor.Instance, (userEnd - userStart) * 100d / results.GcStats.TotalOperations);
		yield return new Metric(CpuPrivilegedMetricDescriptor.Instance, (privEnd - privStart) * 100d / results.GcStats.TotalOperations);
	}

	public IEnumerable<ValidationError> Validate(ValidationParameters validationParameters)
	{
		yield break;
	}

	class CpuUserMetricDescriptor : IMetricDescriptor
	{
		internal static readonly IMetricDescriptor Instance = new CpuUserMetricDescriptor();
		public bool GetIsAvailable(Metric metric) => true;

		public string Id => "CPU User Time";
		public string DisplayName => Id;
		public string Legend => Id;
		public string NumberFormat => "0.##";
		public UnitType UnitType => UnitType.Time;
		public string Unit => "ns";
		public bool TheGreaterTheBetter => false;
		public int PriorityInCategory => 1;
	}

	class CpuPrivilegedMetricDescriptor : IMetricDescriptor
	{
		internal static readonly IMetricDescriptor Instance = new CpuPrivilegedMetricDescriptor();

		public bool GetIsAvailable(Metric metric) => true;

		public string Id => "CPU Privileged Time";
		public string DisplayName => Id;
		public string Legend => Id;
		public string NumberFormat => "0.##";
		public UnitType UnitType => UnitType.Time;
		public string Unit => "ns";
		public bool TheGreaterTheBetter => false;
		public int PriorityInCategory => 1;
	}
}
