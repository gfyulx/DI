package com.gfyulx.DI.schedule.util;


import java.io.*;
import java.util.List;


public class ProcessMethods extends DefaultMethodsSupport {

    /**
     * An alias method so that a process appears similar to System.out, System.in, System.err;
     * you can use process.in, process.out, process.err in a similar fashion.
     *
     * @param self a Process instance
     * @return the InputStream for the process
     * @since 1.0
     */
    public static InputStream getIn(Process self) {
        return self.getInputStream();
    }

    /**
     * Read the text of the output stream of the Process.
     * Closes all the streams associated with the process after retrieving the text.
     *
     * @param self a Process instance
     * @return the text of the output
     * @throws IOException if an IOException occurs.
     * @since 1.0
     */
    /**
     public static String getText(Process self) throws IOException {
     String text = IOMethods.getText(new BufferedReader(new InputStreamReader(self.getInputStream())));
     closeStreams(self);
     return text;
     }
     **/

    /**
     * An alias method so that a process appears similar to System.out, System.in, System.err;
     * you can use process.in, process.out, process.err in a similar fashion.
     *
     * @param self a Process instance
     * @return the error InputStream for the process
     * @since 1.0
     */
    public static InputStream getErr(Process self) {
        return self.getErrorStream();
    }

    /**
     * An alias method so that a process appears similar to System.out, System.in, System.err;
     * you can use process.in, process.out, process.err in a similar fashion.
     *
     * @param self a Process instance
     * @return the OutputStream for the process
     * @since 1.0
     */
    public static OutputStream getOut(Process self) {
        return self.getOutputStream();
    }

    /**
     * Overloads the left shift operator (&lt;&lt;) to provide an append mechanism
     * to pipe data to a Process.
     *
     * @param self  a Process instance
     * @param value a value to append
     * @return a Writer
     * @throws IOException if an IOException occurs.
     * @since 1.0
     */
 /*   public static Writer leftShift(Process self, Object value) throws IOException {
        return IOGroovyMethods.leftShift(self.getOutputStream(), value);
    }*/

    /**
     * Overloads the left shift operator to provide an append mechanism
     * to pipe into a Process
     *
     * @param self  a Process instance
     * @param value data to append
     * @return an OutputStream
     * @throws IOException if an IOException occurs.
     * @since 1.0
     */
  /*  public static OutputStream leftShift(Process self, byte[] value) throws IOException {
        return IOGroovyMethods.leftShift(self.getOutputStream(), value);
    }*/

    /**
     * Wait for the process to finish during a certain amount of time, otherwise stops the process.
     *
     * @param self           a Process
     * @param numberOfMillis the number of milliseconds to wait before stopping the process
     * @throws InterruptedException
     * @since 1.0
     */
    public static int waitForOrKill(Process self, long numberOfMillis, boolean isWaitFor) throws InterruptedException {
        ProcessRunner runnable = new ProcessRunner(self);
        Thread thread = new Thread(runnable);
        thread.start();
        return runnable.waitForOrKill(numberOfMillis, isWaitFor);
    }

    /**
     public static int waitForOrKill(Process self, long numberOfMillis, boolean isWaitFor,double count) throws InterruptedException {
     System.out.println(Thread.currentThread().getName() + " �ȴ����ٽ���:" + count);
     ProcessRunner runnable = new ProcessRunner(self);
     Thread thread = new Thread(runnable);
     thread.start();
     return runnable.waitForOrKill(numberOfMillis,isWaitFor,count,Thread.currentThread().getName());
     }
     **/

    /**
     * Closes all the streams associated with the process (ignoring any IOExceptions).
     *
     * @param self a Process
     * @since 2.1
     */
    public static void closeStreams(Process self) {
        try {
            self.getErrorStream().close();
        } catch (IOException ignore) {
        }
        try {
            self.getInputStream().close();
        } catch (IOException ignore) {
        }
        try {
            self.getOutputStream().close();
        } catch (IOException ignore) {
        }
    }

    /**
     public static void closeStreams(Process self, double count) {
     closeStreams(self);
     System.out.println(Thread.currentThread().getName() + " �ر�:" + count);
     }
     **/
    /**
     * Gets the output and error streams from a process and reads them
     * to keep the process from blocking due to a full output buffer.
     * The stream data is thrown away but blocking due to a full output buffer is avoided.
     * Use this method if you don't care about the standard or error output and just
     * want the process to run silently - use carefully however, because since the stream
     * data is thrown away, it might be difficult to track down when something goes wrong.
     * For this, two Threads are started, so this method will return immediately.
     *
     * @param self a Process
     * @throws Exception
     * @since 1.0
     */
    /*
    public static void consumeProcessOutput(Process self) throws Exception {
        consumeProcessOutput(self, (OutputStream)null, (OutputStream)null);
    }
	*/

    /**
     * Gets the output and error streams from a process and reads them
     * to keep the process from blocking due to a full output buffer.
     * The processed stream data is appended to the supplied Appendable.
     * For this, two Threads are started, so this method will return immediately.
     *
     * @param self   a Process
     * @param output an Appendable to capture the process stdout
     * @param error  an Appendable to capture the process stderr
     * @since 1.7.5
     */
    public static void consumeProcessOutput(Process self, Appendable output, Appendable error) {
        consumeProcessOutputStream(self, output);
        consumeProcessErrorStream(self, error);
    }

    /**
     * Gets the output and error streams from a process and reads them
     * to keep the process from blocking due to a full output buffer.
     * The processed stream data is appended to the supplied OutputStream.
     * For this, two Threads are started, so this method will return immediately.
     *
     * @param self a Process
     * @param output an OutputStream to capture the process stdout
     * @param error an OutputStream to capture the process stderr
     * @throws Exception
     * @since 1.5.2
     */
    /**
     public static void consumeProcessOutput(Process self, OutputStream output, OutputStream error) throws Exception {
     consumeProcessOutputStream(self, output);
     consumeProcessErrorStream(self, error);
     }
     **/
    /**
     public static void consumeProcessOutput(Process self, OutputStream output, OutputStream error,double count) throws Exception {
     consumeProcessOutputStream(self, output,count);
     consumeProcessErrorStream(self, error, count);
     }
     **/
    /**
     * Gets the output and error streams from a process and reads them
     * to keep the process from blocking due to a full output buffer.
     * The stream data is thrown away but blocking due to a full output buffer is avoided.
     * Use this method if you don't care about the standard or error output and just
     * want the process to run silently - use carefully however, because since the stream
     * data is thrown away, it might be difficult to track down when something goes wrong.
     * For this, two Threads are started, but join()ed, so we wait.
     * As implied by the waitFor... name, we also wait until we finish
     * as well. Finally, the output and error streams are closed.
     *
     * @param self a Process
     * @throws Exception
     * @since 1.6.5
     */
    public static void waitForProcessOutput(Process self) throws Exception {
        waitForProcessOutput(self, (OutputStream) null, (OutputStream) null);
    }

    /**
     * Gets the output and error streams from a process and reads them
     * to keep the process from blocking due to a full output buffer.
     * The processed stream data is appended to the supplied Appendable.
     * For this, two Threads are started, but join()ed, so we wait.
     * As implied by the waitFor... name, we also wait until we finish
     * as well. Finally, the input, output and error streams are closed.
     *
     * @param self   a Process
     * @param output an Appendable to capture the process stdout
     * @param error  an Appendable to capture the process stderr
     * @since 1.7.5
     */
    public static void waitForProcessOutput(Process self, Appendable output, Appendable error) {
        Thread tout = consumeProcessOutputStream(self, output);
        Thread terr = consumeProcessErrorStream(self, error);
        try {
            tout.join();
        } catch (InterruptedException ignore) {
        }
        try {
            terr.join();
        } catch (InterruptedException ignore) {
        }
        try {
            self.waitFor();
        } catch (InterruptedException ignore) {
        }
        closeStreams(self);
    }

    /**
     * Gets the output and error streams from a process and reads them
     * to keep the process from blocking due to a full output buffer.
     * The processed stream data is appended to the supplied OutputStream.
     * For this, two Threads are started, but join()ed, so we wait.
     * As implied by the waitFor... name, we also wait until we finish
     * as well. Finally, the input, output and error streams are closed.
     *
     * @param self   a Process
     * @param output an OutputStream to capture the process stdout
     * @param error  an OutputStream to capture the process stderr
     * @throws Exception
     * @since 1.6.5
     */
    public static void waitForProcessOutput(Process self, OutputStream output, OutputStream error) throws Exception {
        Thread tout = consumeProcessOutputStream(self, output);
        Thread terr = consumeProcessErrorStream(self, error);
        try {
            tout.join();
        } catch (InterruptedException ignore) {
        }
        try {
            terr.join();
        } catch (InterruptedException ignore) {
        }
        try {
            self.waitFor();
        } catch (InterruptedException ignore) {
        }
        closeStreams(self);
    }

    /**
     * Gets the error stream from a process and reads it
     * to keep the process from blocking due to a full buffer.
     * The processed stream data is appended to the supplied OutputStream.
     * A new Thread is started, so this method will return immediately.
     *
     * @param self a Process
     * @param err  an OutputStream to capture the process stderr
     * @return the Thread
     * @throws Exception
     * @since 1.5.2
     */
    public static Thread consumeProcessErrorStream(Process self, OutputStream err) throws Exception {
        try {
            Thread thread = new Thread(new ByteDumper(self.getErrorStream(), err));
            thread.start();
            return thread;
        } catch (RuntimeException e) {
//    		System.out.println("Runtime Exception....A");
            throw new Exception(e);
        }
    }

    /**
     public static Thread consumeProcessErrorStream(Process self, OutputStream err,double count) throws Exception {
     try
     {
     Thread thread = new Thread(new ByteDumper(self.getErrorStream(), err, count,Thread.currentThread().getName()));
     thread.start();
     return thread;
     }
     catch (RuntimeException e)
     {
     System.out.println(Thread.currentThread().getName() + " Runtime Exception...." + count);
     throw new Exception(e);
     }
     }
     **/

    /**
     * Gets the error stream from a process and reads it
     * to keep the process from blocking due to a full buffer.
     * The processed stream data is appended to the supplied Appendable.
     * A new Thread is started, so this method will return immediately.
     *
     * @param self  a Process
     * @param error an Appendable to capture the process stderr
     * @return the Thread
     * @since 1.7.5
     */
    public static Thread consumeProcessErrorStream(Process self, Appendable error) {
        Thread thread = new Thread(new TextDumper(self.getErrorStream(), error));
        thread.start();
        return thread;
    }

    /**
     * Gets the output stream from a process and reads it
     * to keep the process from blocking due to a full output buffer.
     * The processed stream data is appended to the supplied Appendable.
     * A new Thread is started, so this method will return immediately.
     *
     * @param self   a Process
     * @param output an Appendable to capture the process stdout
     * @return the Thread
     * @since 1.7.5
     */
    public static Thread consumeProcessOutputStream(Process self, Appendable output) {
        Thread thread = new Thread(new TextDumper(self.getInputStream(), output));
        thread.start();
        return thread;
    }

    /**
     * Gets the output stream from a process and reads it
     * to keep the process from blocking due to a full output buffer.
     * The processed stream data is appended to the supplied OutputStream.
     * A new Thread is started, so this method will return immediately.
     *
     * @param self   a Process
     * @param output an OutputStream to capture the process stdout
     * @return the Thread
     * @throws Exception
     * @since 1.5.2
     */
    public static Thread consumeProcessOutputStream(Process self, OutputStream output) throws Exception {
        try {
            Thread thread = new Thread(new ByteDumper(self.getInputStream(), output));
            thread.start();
            return thread;
        } catch (Exception e) {
            throw new Exception(e);
        }

    }

    /**
     public static Thread consumeProcessOutputStream(Process self, OutputStream output, double count) throws Exception {
     try
     {
     Thread thread = new Thread(new ByteDumper(self.getInputStream(), output,count,Thread.currentThread().getName()));
     thread.start();
     return thread;
     }
     catch (Exception e)
     {
     System.out.println(Thread.currentThread().getName() + " Runtime Exception...." + count);
     throw new Exception(e);
     }

     }
     **/

    /**
     * Creates a new BufferedWriter as stdin for this process,
     * passes it to the closure, and ensures the stream is flushed
     * and closed after the closure returns.
     * A new Thread is started, so this method will return immediately.
     *
     * @param self a Process
     * @param closure a closure
     * @since 1.5.2
     */
    /**
     public static void withWriter(final Process self, final Closure closure) {
     new Thread(new Runnable() {
     public void run() {
     try {
     IOGroovyMethods.withWriter(new BufferedOutputStream(getOut(self)), closure);
     } catch (IOException e) {
     throw new GroovyRuntimeException("exception while reading process stream", e);
     }
     }
     }).start();
     }
     **/
    /**
     * Creates a new buffered OutputStream as stdin for this process,
     * passes it to the closure, and ensures the stream is flushed
     * and closed after the closure returns.
     * A new Thread is started, so this method will return immediately.
     *
     * @param self a Process
     * @param closure a closure
     * @since 1.5.2
     */
    /**
     public static void withOutputStream(final Process self, final Closure closure) {
     new Thread(new Runnable() {
     public void run() {
     try {
     IOGroovyMethods.withStream(new BufferedOutputStream(getOut(self)), closure);
     } catch (IOException e) {
     throw new GroovyRuntimeException("exception while reading process stream", e);
     }
     }
     }).start();
     }
     **/
    /**
     * Allows one Process to asynchronously pipe data to another Process.
     *
     * @param left  a Process instance
     * @param right a Process to pipe output to
     * @return the second Process to allow chaining
     * @throws IOException if an IOException occurs.
     * @since 1.5.2
     */
    public static Process pipeTo(final Process left, final Process right) throws IOException {
        new Thread(new Runnable() {
            public void run() {
                InputStream in = new BufferedInputStream(getIn(left));
                OutputStream out = new BufferedOutputStream(getOut(right));
                byte[] buf = new byte[8192];
                int next;
                try {
                    while ((next = in.read(buf)) != -1) {
                        out.write(buf, 0, next);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("exception while reading process stream", e);
                } finally {
                    closeWithWarning(out);
                }
            }
        }).start();
        return right;
    }

    /**
     * Overrides the or operator to allow one Process to asynchronously
     * pipe data to another Process.
     *
     * @param left  a Process instance
     * @param right a Process to pipe output to
     * @return the second Process to allow chaining
     * @throws IOException if an IOException occurs.
     * @since 1.5.1
     */
    public static Process or(final Process left, final Process right) throws IOException {
        return pipeTo(left, right);
    }

    /**
     * A Runnable which waits for a process to complete together with a notification scheme
     * allowing another thread to wait a maximum number of seconds for the process to complete
     * before killing it.
     *
     * @since 1.0
     */
    protected static class ProcessRunner implements Runnable {
        Process process;
        private boolean finished;

        public ProcessRunner(Process process) {
            this.process = process;
        }

        private void doProcessWait() {
            try {
                process.waitFor();
            } catch (InterruptedException e) {
                // Ignore
            }
        }

        public void run() {
            doProcessWait();
            synchronized (this) {
                notifyAll();
                finished = true;
            }
        }

        /**
         * ��ʱ�Ժ������ӽ���
         *
         * @param millis    ��ʱʱ��
         * @param isWaitFor �����ӽ��̺��Ƿ�ProcessWait
         * @throws InterruptedException
         */
        public synchronized int waitForOrKill(long millis, boolean isWaitFor) throws InterruptedException {
            if (!finished) {
                try {
                    wait(millis);
                } catch (InterruptedException e) {
                    // Ignore
                }

                if (!finished) {
                    process.destroy();
                    if (isWaitFor)
                        doProcessWait();
                    return -1;
                } else
                    return process.exitValue();
            }
            return process.exitValue();
        }

        /**
         public synchronized int waitForOrKill(long millis, boolean isWaitFor,double count, String threadName) throws InterruptedException {
         if (!finished) {
         try {
         wait(millis);
         } catch (InterruptedException e) {
         // Ignore
         }

         if (!finished) {
         process.destroy();
         if (isWaitFor)
         doProcessWait();
         System.out.println(threadName+" �ȴ�" + millis/1000 + "s ��δ����,destroy.. "+ count);
         return -1;
         }
         else
         {
         System.out.println(threadName+" �ȴ�" + millis/1000 + "s �����... "+ count);
         return process.exitValue();
         }
         }
         return process.exitValue();
         }
         **/
    }

    private static class TextDumper implements Runnable {
        InputStream in;
        Appendable app;

        public TextDumper(InputStream in, Appendable app) {
            this.in = in;
            this.app = app;
        }

        public void run() {
            InputStreamReader isr = new InputStreamReader(in);
            BufferedReader br = new BufferedReader(isr);
            String next;
            try {
                while ((next = br.readLine()) != null) {
                    if (app != null) {
                        app.append(next);
                        app.append("\n");
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("exception while reading process stream", e);
            }
        }
    }

    private static class ByteDumper implements Runnable {
        InputStream in;
        OutputStream out;
        double count = 0;
        String threadName;

        public ByteDumper(InputStream in, OutputStream out) {
            this.in = new BufferedInputStream(in);
            this.out = out;
        }

        public void run() {
            byte[] buf = new byte[8192];
            int next;
            try {
                while ((next = in.read(buf)) != -1) {
                    if (out != null) out.write(buf, 0, next);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Executes the command specified by <code>self</code> as a command-line process.
     * <p>For more control over Process construction you can use
     * <code>java.lang.ProcessBuilder</code> (JDK 1.5+).</p>
     *
     * @param self a command line String
     * @return the Process which has just started for this command line representation
     * @throws IOException if an IOException occurs.
     * @since 1.0
     */
    public static Process execute(final String self) throws IOException {
        return Runtime.getRuntime().exec(self);
    }

    /**
     * Executes the command specified by <code>self</code> with environment defined by <code>envp</code>
     * and under the working directory <code>dir</code>.
     * <p>For more control over Process construction you can use
     * <code>java.lang.ProcessBuilder</code> (JDK 1.5+).</p>
     *
     * @param self a command line String to be executed.
     * @param envp an array of Strings, each element of which
     *             has environment variable settings in the format
     *             <i>name</i>=<i>value</i>, or
     *             <tt>null</tt> if the subprocess should inherit
     *             the environment of the current process.
     * @param dir  the working directory of the subprocess, or
     *             <tt>null</tt> if the subprocess should inherit
     *             the working directory of the current process.
     * @return the Process which has just started for this command line representation.
     * @throws IOException if an IOException occurs.
     * @since 1.0
     */
    public static Process execute(final String self, final String[] envp, final File dir) throws IOException {
        return Runtime.getRuntime().exec(self, envp, dir);
    }

    /**
     * Executes the command specified by <code>self</code> with environment defined
     * by <code>envp</code> and under the working directory <code>dir</code>.
     * <p>For more control over Process construction you can use
     * <code>java.lang.ProcessBuilder</code> (JDK 1.5+).</p>
     *
     * @param self a command line String to be executed.
     * @param envp a List of Objects (converted to Strings using toString), each member of which
     *             has environment variable settings in the format
     *             <i>name</i>=<i>value</i>, or
     *             <tt>null</tt> if the subprocess should inherit
     *             the environment of the current process.
     * @param dir  the working directory of the subprocess, or
     *             <tt>null</tt> if the subprocess should inherit
     *             the working directory of the current process.
     * @return the Process which has just started for this command line representation.
     * @throws IOException if an IOException occurs.
     * @since 1.0
     */
    public static Process execute(final String self, final List<String> envp, final File dir) throws IOException {
        return execute(self, stringify(envp), dir);
    }

    /**
     * Executes the command specified by the given <code>String</code> array.
     * The first item in the array is the command; the others are the parameters.
     * <p>For more control over Process construction you can use
     * <code>java.lang.ProcessBuilder</code> (JDK 1.5+).</p>
     *
     * @param commandArray an array of <code>String</code> containing the command name and
     *                     parameters as separate items in the array.
     * @return the Process which has just started for this command line representation.
     * @throws IOException if an IOException occurs.
     * @since 1.0
     */
    public static Process execute(final String[] commandArray) throws IOException {
        return Runtime.getRuntime().exec(commandArray);
    }

    /**
     * Executes the command specified by the <code>String</code> array given in the first parameter,
     * with the environment defined by <code>envp</code> and under the working directory <code>dir</code>.
     * The first item in the array is the command; the others are the parameters.
     * <p>For more control over Process construction you can use
     * <code>java.lang.ProcessBuilder</code> (JDK 1.5+).</p>
     *
     * @param commandArray an array of <code>String</code> containing the command name and
     *                     parameters as separate items in the array.
     * @param envp         an array of Strings, each member of which
     *                     has environment variable settings in the format
     *                     <i>name</i>=<i>value</i>, or
     *                     <tt>null</tt> if the subprocess should inherit
     *                     the environment of the current process.
     * @param dir          the working directory of the subprocess, or
     *                     <tt>null</tt> if the subprocess should inherit
     *                     the working directory of the current process.
     * @return the Process which has just started for this command line representation.
     * @throws IOException if an IOException occurs.
     * @since 1.7.1
     */
    public static Process execute(final String[] commandArray, final String[] envp, final File dir) throws IOException {
        return Runtime.getRuntime().exec(commandArray, envp, dir);
    }

    /**
     * Executes the command specified by the <code>String</code> array given in the first parameter,
     * with the environment defined by <code>envp</code> and under the working directory <code>dir</code>.
     * The first item in the array is the command; the others are the parameters.
     * <p>For more control over Process construction you can use
     * <code>java.lang.ProcessBuilder</code> (JDK 1.5+).</p>
     *
     * @param commandArray an array of <code>String</code> containing the command name and
     *                     parameters as separate items in the array.
     * @param envp         a List of Objects (converted to Strings using toString), each member of which
     *                     has environment variable settings in the format
     *                     <i>name</i>=<i>value</i>, or
     *                     <tt>null</tt> if the subprocess should inherit
     *                     the environment of the current process.
     * @param dir          the working directory of the subprocess, or
     *                     <tt>null</tt> if the subprocess should inherit
     *                     the working directory of the current process.
     * @return the Process which has just started for this command line representation.
     * @throws IOException if an IOException occurs.
     * @since 1.7.1
     */
    public static Process execute(final String[] commandArray, final List<String> envp, final File dir) throws IOException {
        return Runtime.getRuntime().exec(commandArray, stringify(envp), dir);
    }

    /**
     * Executes the command specified by the given list. The toString() method is called
     * for each item in the list to convert into a resulting String.
     * The first item in the list is the command the others are the parameters.
     * <p>For more control over Process construction you can use
     * <code>java.lang.ProcessBuilder</code> (JDK 1.5+).</p>
     *
     * @param commands a list containing the command name and
     *                 parameters as separate items in the list.
     * @return the Process which has just started for this command line representation.
     * @throws IOException if an IOException occurs.
     * @since 1.0
     */
    public static Process execute(final List<String> commands) throws IOException {
        return execute(stringify(commands));
    }

    /**
     * Executes the command specified by the given list,
     * with the environment defined by <code>envp</code> and under the working directory <code>dir</code>.
     * The first item in the list is the command; the others are the parameters. The toString()
     * method is called on items in the list to convert them to Strings.
     * <p>For more control over Process construction you can use
     * <code>java.lang.ProcessBuilder</code> (JDK 1.5+).</p>
     *
     * @param commands a List containing the command name and
     *                 parameters as separate items in the list.
     * @param envp     an array of Strings, each member of which
     *                 has environment variable settings in the format
     *                 <i>name</i>=<i>value</i>, or
     *                 <tt>null</tt> if the subprocess should inherit
     *                 the environment of the current process.
     * @param dir      the working directory of the subprocess, or
     *                 <tt>null</tt> if the subprocess should inherit
     *                 the working directory of the current process.
     * @return the Process which has just started for this command line representation.
     * @throws IOException if an IOException occurs.
     * @since 1.7.1
     */
    public static Process execute(final List<String> commands, final String[] envp, final File dir) throws IOException {
        return Runtime.getRuntime().exec(stringify(commands), envp, dir);
    }

    /**
     * Executes the command specified by the given list,
     * with the environment defined by <code>envp</code> and under the working directory <code>dir</code>.
     * The first item in the list is the command; the others are the parameters. The toString()
     * method is called on items in the list to convert them to Strings.
     * <p>For more control over Process construction you can use
     * <code>java.lang.ProcessBuilder</code> (JDK 1.5+).</p>
     *
     * @param commands a List containing the command name and
     *                 parameters as separate items in the list.
     * @param envp     a List of Objects (converted to Strings using toString), each member of which
     *                 has environment variable settings in the format
     *                 <i>name</i>=<i>value</i>, or
     *                 <tt>null</tt> if the subprocess should inherit
     *                 the environment of the current process.
     * @param dir      the working directory of the subprocess, or
     *                 <tt>null</tt> if the subprocess should inherit
     *                 the working directory of the current process.
     * @return the Process which has just started for this command line representation.
     * @throws IOException if an IOException occurs.
     * @since 1.7.1
     */
    public static Process execute(final List<String> commands, final List<String> envp, final File dir) throws IOException {
        return Runtime.getRuntime().exec(stringify(commands), stringify(envp), dir);
    }

    private static String[] stringify(final List<String> orig) {
        if (orig == null) return null;
        String[] result = new String[orig.size()];
        for (int i = 0; i < orig.size(); i++) {
            result[i] = orig.get(i).toString();
        }
        return result;
    }

}

