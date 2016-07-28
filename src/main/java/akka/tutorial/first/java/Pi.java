package akka.tutorial.first.java;

import akka.actor.*;
import akka.routing.RoundRobinRouter;
import akka.util.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 *
 */
public class Pi
{
    public static void main( String[] args )
    {
        System.out.println( "Starting Pi calculation!" );

        Pi pi = new Pi();
        pi.calculate(8, 10000, 10000);
    }

    //Actors and messages...
    static class Calculate {

    }

    static class Work {
        private final int start;
        private final int numberOfElements;

        public Work(int start, int numberOfElements) {
            this.start = start;
            this.numberOfElements = numberOfElements;
        }

        public int getStart() {
            return start;
        }

        public int getNumberOfElements() {
            return numberOfElements;
        }
    }

    static class Result {
        private final double value;

        public Result(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }
    }

    static class PiApproximation {
        private final double pi;
        private final Duration duration;

        public PiApproximation(double pi, Duration duration) {
            this.pi = pi;
            this.duration = duration;
        }

        public double getPi() {
            return pi;
        }

        public Duration getDuration() {
            return duration;
        }
    }

    public static class Worker extends UntypedActor {

        private double calculatePiFor(int start, int numberOfElements) {
            double acc = 0.0;

            for(int i = start * numberOfElements; i <= ((start + 1) * numberOfElements -1 ); i++) {
                acc += 4.0 * (1 - (i % 2) * 2) / (2 * i + 1);
            }
            return acc;
        }

        public void onReceive(Object message) {
            if(message instanceof Work) {
                Work work = (Work) message;
                double result = calculatePiFor(work.getStart(), work.getNumberOfElements());
                //send it back to the original sender...
                getSender().tell(new Result(result), getSelf());
            } else {
                unhandled(message);
            }

        }
    }

    public static class Master extends UntypedActor {
        private final int numberOfMessages;
        private final int numberOfElements;

        private double pi;
        private int numberOfResults;
        private final long start = System.currentTimeMillis();

        private final ActorRef listener;
        private final ActorRef workerRouter;

        public Master(final int numberOfWorkers, int numberOfMessages,
                      int numberOfElements, ActorRef listener) {
            this.numberOfMessages = numberOfMessages;
            this.numberOfElements = numberOfElements;
            this.listener = listener;

            workerRouter = this.getContext().actorOf(new Props(Worker.class).withRouter(new RoundRobinRouter(numberOfWorkers)),
                    "workerRouter");
        }

        public void onReceive(Object message) {
            //handle messages
            if(message instanceof Calculate) {
                for (int start = 0; start < numberOfMessages; start++) {
                    workerRouter.tell(new Work(start, numberOfElements), getSelf());
                }
            } else if(message instanceof Result) {
                Result result = (Result) message;
                pi += result.getValue();
                numberOfResults += 1;
                if(numberOfResults == numberOfMessages) {
                    //send the results to the listener
                    Duration  duration = Duration.create(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                    listener.tell(new PiApproximation(pi, duration), getSelf());
                    getContext().stop(getSelf());
                } else {
                    unhandled(message);
                }
            }
        }
    } //end Master

    public static class Listener extends UntypedActor {
        public void onReceive(Object message) {
            if(message instanceof PiApproximation) {
                PiApproximation approximation = (PiApproximation) message;
                System.out.println(String.format("\n\tPi approximation: \t\t%s\n\tCalculation time: \t%s",
                        approximation.getPi(), approximation.getDuration()));
                getContext().system().shutdown();
            } else {
                unhandled(message);
            }
        }
    }
    // End actors and messages

    public void calculate(final int numberOfWorkers, final int numberOfElements, final int numberOfMessages) {
        System.out.println("Number of workers: " + numberOfWorkers);

        //Create an Akka system
        ActorSystem system = ActorSystem.create("PiSystem");

        //create the result listener, which will print the result and shutdown the system
        final ActorRef listener = system.actorOf(new Props(Listener.class), "listener");

        //create the master
        ActorRef master = system.actorOf(new Props(new UntypedActorFactory() {
            public UntypedActor create() {
                return new Master(numberOfWorkers, numberOfMessages, numberOfElements, listener);
            }
        }), "master");

        //start the calculation
        master.tell(new Calculate());
    }

}


