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

/**
 * A HashStreamException is thrown if an operation fails in the HashStream implementation.
 * @author Caleb Shingledecker
 *
 */
public class HashStreamException extends MachinicDataException {
	private static final long serialVersionUID = 8180475873507415054L;
	
	public HashStreamException(String message) {
		super(message);
	}
	
    public HashStreamException(final String message, String... args) {
    super(message,args);
    }
	
    public HashStreamException(final String message, final Throwable cause) {
    super(message, cause);
    }
    public HashStreamException(final String message, final Throwable cause,String... args) {
    super(message,cause, args);
    }
}
