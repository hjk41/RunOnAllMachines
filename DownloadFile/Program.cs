using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;

namespace DownloadFile
{
    class Program
    {
        static int numParallelDownload = 8;
        static SortedDictionary<int, ProcessAndInfo> finishedProcesses = new SortedDictionary<int,ProcessAndInfo>();
        static List<ProcessAndInfo> runningProcesses = new List<ProcessAndInfo>();

        class ProcessAndInfo
        {
            public Process process;
            public int idx;
            public string cmd;
            public StreamReader stdout;
            public StreamReader stderr;
        };
        static void FinishOneProcess(ProcessAndInfo pinfo)
        {
            Console.WriteLine("****************************************");
            Console.WriteLine(pinfo.idx + " : " + pinfo.process.ExitCode + " : " + pinfo.cmd);
            Console.WriteLine("================= stdout ===============");
            Console.WriteLine(pinfo.stdout.ReadToEnd());
            Console.WriteLine("================= stderr ===============");
            Console.Write(pinfo.stderr.ReadToEnd());
            finishedProcesses.Add(pinfo.idx, pinfo);
            runningProcesses.Remove(pinfo);
        }

        static void Main(string[] args)
        {
            if (args.Length < 5)
            {
                Console.WriteLine("usage: DownloadFile prefix localDir numParts numWorkers workerId");
                return;
            }
            string prefix = args[0];
            string localDir = args[1];
            int nTrainParts = int.Parse(args[2]);
            int nParts = int.Parse(args[3]);
            int partId = int.Parse(args[4]);

            int partPerNode = Math.Max((nTrainParts + nParts - 1) / nParts, 1);
            int partBegin = Math.Min(partPerNode * partId, nTrainParts);
            int partEnd = Math.Min(partPerNode * (partId + 1), nTrainParts);

            
            for (int i = partBegin; i < partEnd; i++)
            {
                while (runningProcesses.Count >= numParallelDownload)
                {
                    var pr = runningProcesses.ElementAt(0);
                    pr.process.WaitForExit();
                    FinishOneProcess(pr);
                }
                string path = prefix + "/part-" + i.ToString("00000");
                string cmd = "hdfs dfs -get " + path + " " + localDir + "\\";
                Console.WriteLine(cmd);
                var process = new System.Diagnostics.Process();
                process.StartInfo.UseShellExecute = false;
                process.StartInfo.RedirectStandardOutput = true;
                process.StartInfo.RedirectStandardError = true;
                process.StartInfo.FileName = @"cmd.exe";
                process.StartInfo.Arguments = "/c " + cmd;
                process.Start();
                ProcessAndInfo pinfo = new ProcessAndInfo();
                pinfo.process = process;
                pinfo.idx = i;
                pinfo.cmd = cmd;
                pinfo.stdout = process.StandardOutput;
                pinfo.stderr = process.StandardError;
                runningProcesses.Add(pinfo);
            }
            while (runningProcesses.Count > 0)
            {
                foreach (var pinfo in runningProcesses.ToArray())
                {
                    pinfo.process.WaitForExit();
                    FinishOneProcess(pinfo);
                }
            }

            Console.WriteLine("****************************************");
            Console.Write("failures: ");
            int failures = 0;
            foreach (var kv in finishedProcesses)
            {
                int idx = kv.Key;
                ProcessAndInfo pinfo = kv.Value;
                if (pinfo.process.ExitCode != 0)
                {
                    failures++;
                    Console.Write("\t" + idx);
                }
            }

            System.Environment.Exit(failures);
        }
    }
}
