package com.consumer.consume.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import static com.consumer.consume.constants.ApplicationConstants.MB;
import static com.consumer.consume.constants.ApplicationConstants.WEB_TOPIC;

@Service
public class WebEventConsumer {
    @Value("${file.size-threshold}")
    private long sizeThreshold;

    @Value("${file.time-threshold}")
    private Integer minutesThreshold;

    private Map<String, File> fileMap;

    public WebEventConsumer() {
        fileMap = new HashMap<>();
    }

    @KafkaListener(topics = {WEB_TOPIC})
    public void onMessage(ConsumerRecord record) throws IOException {
        String dataVersion = record.value().toString().split(";")[0];
        System.out.println(dataVersion);

        //if file not present for current dataVersion
        if (!fileMap.containsKey(dataVersion))
            fileMap.put(dataVersion, new File(WEB_TOPIC + "_" + dataVersion + "_" + Instant.now().toString()));

        //if file size is within specified file size
        if (fileMap.get(dataVersion).length() > (sizeThreshold * Integer.parseInt(MB)))
            fileMap.put(dataVersion, new File(WEB_TOPIC + "_" + dataVersion + "_" + Instant.now().toString()));

        //if file time is expired
        Instant fileStartTime = Instant.parse(fileMap.get(dataVersion).getName().split("_")[2]);
        if (Instant.now().isAfter(fileStartTime.plus(minutesThreshold, ChronoUnit.MINUTES)))
            fileMap.put(dataVersion, new File(WEB_TOPIC + "_" + dataVersion + "_" + Instant.now().toString()));

        ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(fileMap.get(dataVersion), true));
        out.writeUTF(record.value().toString().split(";")[1]);
        out.flush();
        out.close();

    }
}
