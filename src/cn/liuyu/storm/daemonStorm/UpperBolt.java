package cn.liuyu.storm.daemonStorm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


//bolt������߼�����Ԫ��������Ϣ�����д���
public class UpperBolt extends BaseBasicBolt {
	
	//������bolt���Ҫ���ͳ�ȥ��tuple�ֶ�
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("upperName"));
	}

	//��Ҫ��ҵ���߼���������һ����������������ݣ�����ҵ����������tuple��
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		
		String stringTupleWord = tuple.getString(0);   //��ȡ����
		
		String upperWord=stringTupleWord.toUpperCase(); //������ת���ɴ�д
		
		//�����ݷ�װ��tuple,���з���
		
		collector.emit(new Values(upperWord));
	}

}
