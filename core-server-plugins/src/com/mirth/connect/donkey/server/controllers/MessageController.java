package com.mirth.connect.donkey.server.controllers;

import java.util.Map;
import java.util.Set;

import com.mirth.connect.donkey.model.message.Message;
import com.mirth.connect.donkey.model.message.Status;
import com.mirth.connect.donkey.model.message.attachment.Attachment;

public abstract class MessageController {

	public abstract Attachment createAttachment(Object data, String type) throws UnsupportedDataTypeException;

	public abstract Attachment createAttachment(Object data, String type, boolean base64Encode) throws UnsupportedDataTypeException;

	public abstract void insertAttachment(Attachment attachment, String channelId, Long messageId);

	public abstract void updateAttachment(Attachment attachment, String channelId, Long messageId);

	public abstract boolean isMessageCompleted(Message message);

	public abstract boolean isMessageCompleted(Set<Status> statuses);

	public abstract void deleteMessages(String channelId, Map<Long, Set<Integer>> messages);
	
}
