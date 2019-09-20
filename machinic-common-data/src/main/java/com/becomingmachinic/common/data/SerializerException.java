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

package com.becomingmachinic.common.data;

public class SerializerException extends MachinicDataException {
	private static final long serialVersionUID = 8180475873507415054L;
	
	public SerializerException(String message) {
		super(message);
	}
	
    public SerializerException(final String message, String... args) {
    super(message,args);
    }
	
    public SerializerException(final String message, final Throwable cause) {
    super(message, cause);
    }
    public SerializerException(final String message, final Throwable cause,String... args) {
    super(message,cause, args);
    }
}
