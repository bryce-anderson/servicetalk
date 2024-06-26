/*
 * Copyright © 2024 Apple Inc. and the ServiceTalk project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.servicetalk.capacity.limiter.api;

import javax.annotation.Nullable;

/**
 * An {@link Exception} to indicate that a request was dropped by a client/server likely due to capacity constraints or
 * as-such interpretation of peer feedback according to configuration.
 */
public class RequestDroppedException extends RuntimeException {

    private static final long serialVersionUID = 2152182132883133067L;

    /**
     * Creates a new instance.
     */
    public RequestDroppedException() {
    }

    /**
     * Creates a new instance.
     *
     * @param message the detail message.
     */
    public RequestDroppedException(@Nullable final String message) {
        super(message);
    }

    /**
     * Creates a new instance.
     *
     * @param message the detail message.
     * @param cause of this exception.
     */
    public RequestDroppedException(@Nullable final String message, @Nullable final Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new instance.
     *
     * @param cause of this exception.
     */
    public RequestDroppedException(@Nullable final Throwable cause) {
        super(cause);
    }

    /**
     * Creates a new instance.
     *
     * @param message the detail message.
     * @param cause of this exception.
     * @param enableSuppression {@code true} if suppression should be enabled.
     * @param writableStackTrace {@code true} if the stack trace should be writable
     */
    public RequestDroppedException(@Nullable final String message, @Nullable final Throwable cause,
                                   final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
