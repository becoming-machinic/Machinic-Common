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

public class MachinicDataException extends Exception {
	private static final long serialVersionUID = 1012248866681603163L;
	
	public MachinicDataException(String message) {
		super(message);
	}
	
	/**
	 * 
	 * @param message
	 * @param args
	 */
    public MachinicDataException(final String message, String... args) {
    super(String.format(message, (Object[]) args));;
    }
	
    /**
     * Construct a new exception with the specified detail message and cause.
     * 
     * @param message
     *            The detail message
     * @param exitValue The exit value
     * @param cause
     *            The underlying cause
     */
    public MachinicDataException(final String message, final Throwable cause) {
    super(String.format("%s. Caused by %s", message, cause.getMessage()), cause);
    }
    public MachinicDataException(final String message, final Throwable cause,String... args) {
    super(String.format("%s. Caused by %s", String.format(message, (Object[]) args), cause.getMessage()), cause);
    }
}
