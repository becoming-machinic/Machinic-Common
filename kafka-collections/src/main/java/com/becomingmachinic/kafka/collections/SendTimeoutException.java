/*
 * Copyright (C) 2019 Becoming Machinic Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.becomingmachinic.kafka.collections;

public class SendTimeoutException extends KafkaCollectionException {
	private static final long serialVersionUID = -159940199543531986L;
	
	public SendTimeoutException(String message) {
		super(message);
	}
	
	public SendTimeoutException(String message, Throwable cause) {
		super(String.format("%s. Caused by %s", message, cause.getMessage()), cause);
	}
	
	public SendTimeoutException(String message, String... vars) {
		this(String.format(message, (Object[]) vars));
	}
	
	public SendTimeoutException(String message, Throwable cause, String... vars) {
		this(String.format(message, (Object[]) vars), cause);
	}
	
}
