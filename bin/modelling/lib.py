from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error


def evaluate_arima_model(X, arima_order):
    """
    Evaluate an ARIMA model for a given order (p,d,q)
    :param X: list or series containing all historical data
    :param arima_order: Tuple with 3 integers representing the order for the model
    :return: mse (error metric) and the fitted model
    """
    # Prepare training dataset
    train_size = int(len(X) * 0.75)
    train, test = X[0:train_size], X[train_size:]
    history = [x for x in train]

    # Make predictions
    predictions = list()
    for t in range(len(test)):
        # Fit model
        model = ARIMA(history, order=arima_order)
        model_fit = model.fit(disp=0)

        # Forecast
        yhat = model_fit.forecast()[0]

        # Store prediction and move forward one time step
        predictions.append(yhat)
        history.append(test[t])

    # calculate out of sample error
    mse = mean_squared_error(test, predictions)
    return mse, model_fit


def evaluate_models(X, p_values, d_values, q_values):
    """
    Evaluate all combinations of p, d and q values for an ARIMA model
    :param X: All historical data for product
    :param p_values: list with all values of p to test
    :param d_values: list with all values of d to test
    :param q_values: list with all values of q to test
    :return: best_cfg (model order with lowest mse), best_score (mse), best_model (fitted model)
    """

    # Initialize helper variables
    X = X.astype('float32')
    best_score, best_cfg, best_model = float("inf"), None, None

    # Loop through all combos
    for p in p_values:
        for d in d_values:
            for q in q_values:

                # Set the order
                order = (p, d, q)

                # Handle exceptions for bad model orders
                try:

                    # Score model on hold out data
                    mse, model = evaluate_arima_model(X, order)

                    # Update best model if error is lower
                    if mse < best_score:
                        best_score, best_cfg, best_model = mse, order, model
                except:
                    continue

    return best_cfg, best_score, best_model
