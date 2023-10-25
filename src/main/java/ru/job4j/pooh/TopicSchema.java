package ru.job4j.pooh;

import java.util.Iterator;
import java.util.concurrent.*;

public class TopicSchema implements Schema {

    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Receiver>> receivers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BlockingQueue<String>> data = new ConcurrentHashMap<>();
    private final Condition condition = new Condition();

    @Override
    public void addReceiver(Receiver receiver) {
        receivers.putIfAbsent(receiver.name(), new CopyOnWriteArrayList<>());
        receivers.get(receiver.name()).add(receiver);
        condition.on();
    }

    @Override
    public void publish(Message message) {
        data.putIfAbsent(message.name(), new LinkedBlockingQueue<>());
        data.get(message.name()).add(message.text());
        condition.on();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            for (String queueKey : data.keySet()) {
                if (receivers.containsKey(queueKey)) {
                    BlockingQueue<String> linkedBlockingQueue = data.get(queueKey);
                    CopyOnWriteArrayList<Receiver> receiver = receivers.get(queueKey);
                    while (!linkedBlockingQueue.isEmpty()) {
                        String text = linkedBlockingQueue.poll();
                        receiver.forEach(s -> s.receive(text));
                    }
                }
            }
            condition.off();
            try {
                condition.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
