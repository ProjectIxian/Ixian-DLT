using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DLT.Meta
{
    public class StatsConsoleScreen
    {

        private static Thread thread = null;
        private static bool running = false;

        public static char[,] screenBufferArray = new char[500, 300];

        public StatsConsoleScreen()
        {
            // Start thread
            running = true;
            thread = new Thread(new ThreadStart(threadLoop));
            thread.Start();

        }

        // Shutdown console thread
        public static void stop()
        {
            running = false;
        }

        private static void threadLoop()
        {
            int a = 0;
            while (running)
            {
                a++;
                drawText(String.Format("Test:\t\t{0}", a), 20, 30);

                drawScreenBuffer();

                Thread.Sleep(1000);

                Thread.Yield();
            }
        }

        public static void drawText(string text, int x, int y)
        {
            Char[] arr = text.ToCharArray(0, text.Length);
            int i = 0;
            foreach (char c in arr)
            {
                screenBufferArray[x + i, y] = c;
                i++;
            }
        }

        private static void drawScreenBuffer()
        {
            Console.SetCursorPosition(0, 0);
            string screenBuffer = "";
            for (int iy = 0; iy < 300 - 1; iy++)
            {
                for (int ix = 0; ix < 500; ix++)
                {
                    screenBuffer += screenBufferArray[ix, iy];
                }
            }
            Console.Write("TEST");
        }

    }
}
