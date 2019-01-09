package sample.cluster.simple;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class SimpleClusterApp {

  public static void main(String[] args) throws Exception {
    if (args.length == 0)
      startup(new String[] { "2551", "2552", "0" });
    else
      startup(args);


    Thread.sleep(5000);
    System.out.println("Start last node");

    final ActorSystem actorSystem = createActorSystem("2553");

    Thread.sleep(5000);
    actorSystem.terminate();


  }

  public static void startup(String[] ports) {
    for (String port : ports) {
      createActorSystem(port);
    }
  }

  private static ActorSystem createActorSystem(String port)
  {
    // Override the configuration of the port
    Config config = ConfigFactory.parseString(
        "akka.remote.artery.canonical.port=" + port)
                                 .withFallback(ConfigFactory.load());

    // Create an Akka system
    ActorSystem system = ActorSystem.create("ClusterSystem", config);

    // Create an actor that handles cluster domain events
    final ActorRef clusterListener = system.actorOf(Props.create(SimpleClusterListener.class), "clusterListener");
    return system;
  }
}
