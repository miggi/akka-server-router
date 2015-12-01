package loader

object SqlStatements {

  val dropWorkersSql = "DROP TABLE if exists public.workers"
  val cleanWorkersSql = "DELETE FROM public.workers"
  val createWorkersSql = "CREATE TABLE public.workers (id integer auto_increment, host char(100), primary key (id), unique(host))"
  val dropDelaySql = "DROP TABLE if exists public.delay"
  val createDelaySql = "CREATE TABLE IF NOT EXISTS public.delay (id integer auto_increment, delay integer, primary key (id))"
  val initDelaySql = "INSERT INTO public.delay (delay) VALUES (1000)"
  val selectDelay = "SELECT * FROM public.delay"
  val selectWorkers = "SELECT * FROM public.workers"

}
