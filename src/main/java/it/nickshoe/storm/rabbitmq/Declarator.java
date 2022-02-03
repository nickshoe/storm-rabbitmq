package it.nickshoe.storm.rabbitmq;

import com.rabbitmq.client.Channel;

import java.io.Serializable;

public interface Declarator extends Serializable {
  void execute(Channel channel);

  public static class NoOp implements Declarator {
	private static final long serialVersionUID = 1L;

	@Override
    public void execute(Channel channel) {}
  }
}
