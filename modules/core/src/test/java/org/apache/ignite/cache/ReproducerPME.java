package org.apache.ignite.cache;import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

public class ReproducerPME {

    static final int NODES = 10;
    public static final int CACHES = 10;

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        System.setProperty("IGNITE_QUIET", "false");
        System.setProperty("IGNITE_UPDATE_NOTIFIER", "false");


        List<Future> futures = new ArrayList<>();
        ExecutorService ex = Executors.newFixedThreadPool(50);

        for (int i = 0; i < 20; i++) {
            final long end = System.currentTimeMillis() + 1000 * 60;
            CountDownLatch latch = new CountDownLatch(NODES);
            futures.add(ex.submit(new StartIgniteTask(false, Duration.ofMillis(50), end, NODES, latch)));
            futures.add(ex.submit(new StartIgniteTask(false, Duration.ofMillis(250), end, NODES, latch)));
            futures.add(ex.submit(new StartIgniteTask(false, Duration.ofSeconds(1), end, NODES, latch)));
            futures.add(ex.submit(new StartIgniteTask(false, Duration.ofSeconds(5), end, NODES, latch)));
            futures.add(ex.submit(new StartIgniteTask(false, Duration.ofSeconds(10), end, NODES, latch)));

            futures.add(ex.submit(new StartIgniteTask(true, Duration.ofMillis(50), end, NODES, latch)));
            futures.add(ex.submit(new StartIgniteTask(true, Duration.ofMillis(250), end, NODES, latch)));
            futures.add(ex.submit(new StartIgniteTask(true, Duration.ofSeconds(1), end, NODES, latch)));
            futures.add(ex.submit(new StartIgniteTask(true, Duration.ofSeconds(5), end, NODES, latch)));
            futures.add(ex.submit(new StartIgniteTask(true, Duration.ofSeconds(10), end, NODES, latch)));

            for (Future future : futures) {
                future.get();
            }
        }

        System.out.println("Finished");

    }

    public static IgniteConfiguration getCfg() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteInstanceName(UUID.randomUUID().toString());

        CacheConfiguration[] ccfg = new CacheConfiguration[CACHES];
        for (int i = 0; i < CACHES; i++) {
            ccfg[i] = new CacheConfiguration("cache" + i);
        }

        cfg.setCacheConfiguration(ccfg);

        cfg.setNetworkTimeout(10000);
        cfg.setClientFailureDetectionTimeout(10000);
        cfg.setFailureDetectionTimeout(10000);

        TcpDiscoverySpi discovery = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();

        finder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));
        discovery.setIpFinder(finder);
        cfg.setDiscoverySpi(discovery);

        return cfg;
    }


}

class StartIgniteTask implements Runnable {
    private boolean client;
    private Duration lifetime;
    private long end;
    private int nodesCnt;
    private CountDownLatch latch;

    public StartIgniteTask(boolean client, Duration lifetime, long end, int nodesCnt, CountDownLatch latch) {
        this.client = client;
        this.lifetime = lifetime;
        this.end = end;
        this.nodesCnt = nodesCnt;
        this.latch = latch;
    }

    @Override
    public void run() {
        try {
            Ignite ignite = null;

            while (System.currentTimeMillis() < end) {
                if (ignite != null) ignite.close();

                ignite = Ignition.start(ReproducerPME.getCfg().setClientMode(client));

                Thread.sleep(lifetime.toMillis());
            }

            latch.countDown();
            latch.await();

//            while (ignite.cluster().nodes().size() < nodesCnt) {
//                Thread.sleep(5000);
//                System.out.println("Waiting..."); // It's possible split brain behaviour
//            }


            if (ignite != null)
                ignite.close();
        } catch (Exception e) {
            e.printStackTrace();

            System.exit(1);
        }
    }
}
