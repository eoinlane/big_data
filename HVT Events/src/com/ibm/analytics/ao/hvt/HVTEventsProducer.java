package com.ibm.analytics.ao.hvt;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.xml.sax.SAXException;

import com.opencsv.CSVReader;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvToBean;

public class HVTEventsProducer {

	// private static final Logger LOG =
	// Logger.getLogger(HVTEventsProducer.class);

	/*
	 * public static void main(String[] args) throws
	 * ParserConfigurationException, SAXException, IOException,
	 * URISyntaxException { if (args.length != 2) {
	 * 
	 * System.out.println("Usage: TruckEventsProducer <broker list> <zookeeper>"
	 * ); System.exit(-1); }
	 */
	public static void main(String[] args)
			throws ParserConfigurationException, SAXException, IOException, URISyntaxException {

		// LOG.debug("Using broker list:" + args[0] + ", zk conn:" + args[1]);

		// long events = Long.parseLong(args[0]);
		Properties props = new Properties();
		props.put("metadata.broker.list", "args[0]");
		props.put("zk.connect", " args[1]");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");

		String TOPIC = "hvtevent";
		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);

		String finalEvent = "";

		String ops_data = "hvt_ops_future.csv";
		String[] array_ops_data = GetCVSList(ops_data);

		// Find max route arraysize.
		int maxarraysize = array_ops_data.length;

		for (int i = 0; i < maxarraysize; i++) {

			finalEvent = new Timestamp(new Date().getTime()) + "|" + getmodelSerialPeriod(array_ops_data[i]);
			try {
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, finalEvent);
				// LOG.info("Sending Messge #: " + routeName[0] + ": " + i + ",
				// msg:" + finalEvent);
				// producer.send(data);
				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		producer.close();
	}

	public static String[] GetCVSList(String urlString) throws IOException {
		CSVReader reader = new CSVReader(new FileReader(urlString));
		// List myEntries = reader.readAll();
		ColumnPositionMappingStrategy strat = new ColumnPositionMappingStrategy();
		strat.setType(YourOrderBean.class);
		String[] columns = new String[] { "model", "serial", "period", "rating", "loadFactor", "load", "damage",
				"maintenanceScheduled", "maintenancePerformed", "highTempDays", "externalRemainingLife",
				"scheduledMaintenanceCost" }; // the fields to bind to in your
												// JavaBean
		strat.setColumnMapping(columns);

		CsvToBean csv = new CsvToBean();
		List list = csv.parse(strat, reader);

		String[] arrList = new String[list.size()];
		arrList = (String[]) list.toArray(arrList);

		return arrList;
	}

	private static String getmodelSerialPeriod(String str) {
		str = str.replace("\t", "");
		str = str.replace("\n", "");

		String[] latLong = str.split("|");

		if (latLong.length == 2) {
			return latLong[1].trim() + "|" + latLong[0].trim();
		} else {
			return str;
		}
	}
}
