package com.certifyos.directory_accuracy.clients;

/** api-layer calls are not possible yet: the Auth0 machine client id and secret are absent. */
public class ApiLayerNotConfiguredException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    public ApiLayerNotConfiguredException() {
        super("api-layer is not configured: API_LAYER_CLIENT_ID and API_LAYER_CLIENT_SECRET are absent");
    }
}
