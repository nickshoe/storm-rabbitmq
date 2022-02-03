package it.nickshoe.storm.rabbitmq;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This is a simple bolt for producing messages to RabbitMQ from a Storm
 * topology. It needs a {@link TupleToMessage} object to perform the real meat
 * of converting the incoming {@link Tuple} from a stream into a {@link Message}
 * to publish on RabbitMQ.
 * 
 * @author bdgould
 *
 */
public class RabbitMQBolt extends BaseRichBolt {

	private static final long serialVersionUID = 97236452008970L;

	private final TupleToMessage scheme;
	private final Declarator declarator;

	private transient Logger logger;
	private transient RabbitMQProducer producer;
	private transient OutputCollector collector;

	public RabbitMQBolt(final TupleToMessage scheme) {
		this(scheme, new Declarator.NoOp());
	}

	public RabbitMQBolt(final TupleToMessage scheme, final Declarator declarator) {
		this.scheme = scheme;
		this.declarator = declarator;
	}

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
		this.logger = LoggerFactory.getLogger(this.getClass());
		
		this.collector = collector;
		this.producer = new RabbitMQProducer(declarator);
		
		this.producer.open(topoConf);
		this.scheme.prepare(topoConf);
		
		this.logger.info("Successfully prepared RabbitMQBolt");
	}

	@Override
	public void execute(final Tuple tuple) {
		this.publish(tuple);
		
		// tuples are always acked, even when transformation by scheme yields Message.NONE
		// as if it failed once it's unlikely to succeed when re-attempted 
		// (i.e. serialization/deserilization errors).
		this.acknowledge(tuple);
	}

	protected void publish(Tuple tuple) {
		Message message = scheme.produceMessage(tuple);
		
		this.producer.send(message);
	}

	protected void acknowledge(Tuple tuple) {
		this.collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		// Since this is a sink, no downstream bolts should be specified. 
		// Thus, it's not necessary to specify the output fields.
	}

}
