package cn.liuyu.storm.daemonStorm;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


//��ϢԴ��spout��������Ϣ���������װ��tuple,����Ϣ��
public class RandomWordSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;  //��Ϣ����ռ���
	
	Values values=new Values();
	
	String[]  words={"xiaomi","meizu","kupai"};
	
	@Override  //��Ҫ��ҵ���߼�����������һ���������tuple��Ϣ
	public void nextTuple() {
		Random random=new Random(); 
		int index=random.nextInt(words.length);  //���������
		
		//ͨ������������ȡ��һ������
		String word=words[index];
		
		//�����ݷ�װ��tuple,���͸���һ�����
		values.add(word);
		collector.emit(values);
		
		//ÿ����һ����Ϣ����Ϣ500ms
		Utils.sleep(500);
	}

	@Override   //��ʼ��,spout�����ʼ��ʱ����һ��
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector=collector;
	}

	@Override  //������Spout������ͳ�ȥ��tuple���ֶ�
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("name"));
	}
	
}
