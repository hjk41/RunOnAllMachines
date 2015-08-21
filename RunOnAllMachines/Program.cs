using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;
using System.Configuration;

namespace RunOnAllMachines
{
    class ProcessAndInfo
    {
        public string machineName;
        public Process process;
        public int machineId;
        public StreamReader stdout;
        public StreamReader stderr;
    };

    class Program
    {
        static int maxNumProcess = 20;
        static string logDir = "Logs";

        static SortedDictionary<int, string> allMachines = new SortedDictionary<int, string>();
        static Dictionary<string, int> finishedProcesses = new Dictionary<string, int>();
        static List<ProcessAndInfo> currentProcesses = new List<ProcessAndInfo>();

        static void FinishOneProcess(ProcessAndInfo pinfo)
        {
            var f = new StreamWriter(logDir + "\\" + pinfo.machineName + ".txt");
            f.WriteLine("================= stdout ===============");
            f.WriteLine(pinfo.stdout.ReadToEnd());
            f.WriteLine("================= stderr ===============");
            f.Write(pinfo.stderr.ReadToEnd());
            f.Close();

            string m = pinfo.machineName;
            int c = pinfo.process.ExitCode;
            finishedProcesses.Add(m, c);
            Console.WriteLine("[{0}/{1}] {2} finished with code {3}, active={4}", finishedProcesses.Count, allMachines.Count, m, c, currentProcesses.Count());
            currentProcesses.Remove(pinfo);
        }

        static void Main(string[] args)
        {
            //ProcessStartInfo si = new ProcessStartInfo();
            //si.FileName = @"cmd.exe";
            //si.Arguments = @"/c robocopy d:\tmp \\srgssd-01\f$\chhong\tmp /MIR";
            //var pr = Process.Start(si);
            //while (!pr.HasExited)
            //{
            //    System.Threading.Thread.Sleep(1000);
            //}
            //return;


            if (args.Length < 2)
            {
                Console.WriteLine("Usage: RunOnAllMachines machineList command [maxNumProcess] [logDir]");
                return;
            }
            if (args.Length >= 3)
                maxNumProcess = int.Parse(args[2]);
            if (args.Length >= 4)
                logDir = args[3];

            if (!Directory.Exists(logDir))
            {
                Directory.CreateDirectory(logDir);
            }

            System.IO.StreamReader file = new System.IO.StreamReader(args[0]);
            string line;
            int counter = 0;
            while ((line = file.ReadLine()) != null)
            {
                counter++;
                var machine = line.Trim();
                if (machine == "") continue;
                machine = machine.Split()[0];
                if (allMachines.ContainsValue(machine))
                {
                    Console.WriteLine("duplicate machine {0} on line {1}", machine, counter);
                    continue;
                }
                allMachines.Add(allMachines.Count, machine);
            }
            file.Close();

            foreach (var item in allMachines)
            {
                var index = item.Key;
                var machine = item.Value;
                while(currentProcesses.Count >= maxNumProcess)
                {
                    foreach (var pinfo in currentProcesses.ToArray())
                    {
                        pinfo.process.WaitForExit();
                        if (pinfo.process.HasExited)
                        {
                            FinishOneProcess(pinfo);
                        }
                        if (currentProcesses.Count < maxNumProcess)
                            break;
                    }
                }
                
                ProcessStartInfo start = new ProcessStartInfo();
                start.FileName = @"cmd.exe";
                start.Arguments = "/c " + args[1];
                start.UseShellExecute = false;
                start.RedirectStandardOutput = true;
                start.RedirectStandardError = true;
                start.EnvironmentVariables["_MachineCount_"] = allMachines.Count.ToString();
                start.EnvironmentVariables["_MachineId_"] = index.ToString();
                start.EnvironmentVariables["_MachineName_"] = machine;
                ProcessAndInfo p = new ProcessAndInfo();
                p.process = Process.Start(start);
                p.machineId = index;
                p.machineName = machine;
                p.stdout = p.process.StandardOutput;
                p.stderr = p.process.StandardError;
                currentProcesses.Add(p);
            }
            while (currentProcesses.Count != 0)
            {
                foreach (var pinfo in currentProcesses.ToArray())
                {
                    pinfo.process.WaitForExit();
                    if (pinfo.process.HasExited)
                    {
                        FinishOneProcess(pinfo);
                    }
                }
            }
            Console.WriteLine("failed machines: ");
            foreach (var mc in finishedProcesses)
            {
                string m = mc.Key;
                int c = mc.Value;
                if (c != 0)
                {
                    Console.Write(m + "\t");
                }
            }
        }

    }
}
