/**
 * subscribe-for-data
 * @module subscribe-for-data
 * @namespace SubscribeForData
 */
const EventEmitter = require('events');

function use(defaultOptions) {
  defaultOptions.parallel = defaultOptions.parallel || 1;
  const bus = new EventEmitter();
  let isFilling = false;
  const awaiting = [];

  /**
   * Creates subscription for related model data
   *
   * @param {Object} source Source model
   * @param {Object} options Options
   * @param {String} options.targetField field data to be saved into (optional)
   * @param {Object} options.baseCondition Base condition
   * @param {*} options.defaultValue Default value for field
   * @param {getKey} options.getKey Callback which returns unique key from target model (model.id by default)
   * @param {getCondition} options.getCondition returns condition, using target model (model.id by default)
   * @param {extractKey} options.extractKey returns unique key of target model from foreign model
   * @param {Boolean} options.isMultiple if one to many relation
   * @param {Boolean} options.useEachAsync only for mongoose cursor
   * @param {Number} options.parallel parallel parameter for `eachAsync` if `useEachAsync` is `true`
   * @param {String} options.foreignField If `getCondition` returns scalar values this field will be used for $in
   * @param {String} options.sourceField field to use of foreign model
   * @param {assignData} options.assignData Do model filling by itself, otherwise use `targetField`
   * @param {getStream} options.getStream returns stream from source and condition (using mongoose model by default)
   * @param {getDataHandler} options.getDataHandler Get data handler for processing related models
   * @param {getAddingMethod} options.getAddingMethod Get `add()` method of future `subscription`
   * @memberOf SubscribeForData
   * @returns {Object}
   */
  function makeSubscription(source, outerOptions) {
    const options = Object.assign({}, defaultOptions, outerOptions);
    const {
      getKey, getCondition, foreignField, baseCondition, defaultValue, targetField, getAddingMethod
    } = options;
    options.extractKey = options.extractKey || (foreign => foreign[foreignField]);
    const targets = {};
    const condition = baseCondition || {};
    const inner = [];
    let isConditionSetup = false;
    awaiting.push({ source, targetField, options, targets, condition });

    return {
      add: getAddingMethod({
        targets, getKey, getCondition, defaultValue, targetField, condition, foreignField, inner, isConditionSetup,
      }),
    };
  }

  /**
   * Fill subscribed targets
   *
   * @memberOf SubscribeForData
   * @returns {Promise}
   */
  async function fillSubscriptions() {
    if (isFilling) await new Promise(resolve => bus.once('released', resolve));

    isFilling = true;
    const promises = awaiting
      .map(({ source, targetField, options: {
          extractKey, sourceField, getStream, isMultiple, useEachAsync, getDataHandler, parallel, assignData
        }, targets, condition }) => useEachAsync ?
        getStream(source, condition).eachAsync(getDataHandler({ targets, extractKey, isMultiple, targetField, sourceField, assignData }),
          { parallel }) :
        new Promise((resolve, reject) => getStream(source, condition)
          .on('data', getDataHandler({ targets, extractKey, isMultiple, targetField, sourceField, assignData }))
          .on('error', reject)
          .on('end', resolve)));
    awaiting.splice(0, awaiting.length);
    isFilling = false;
    bus.emit('released');

    return Promise.all(promises);
  }

  /**
   * change default options
   *
   * @memberOf SubscribeForData
   * @param mixin
   */
  function assignDefaultOptions(mixin) {
    Object.assign(defaultOptions, mixin);
  }

  return {
    fillSubscriptions,
    makeSubscription,
    assignDefaultOptions,
  };

}

module.exports = {
  /**
   * Creates module instance
   *
   * @param {Object} options Options
   * @param {getKey} options.getKey Callback which returns unique key from target model (model.id by default)
   * @param {getStream} options.getStream returns stream from source and condition (using mongoose model by default)
   * @param {getCondition} options.getCondition returns condition, using target model (model.id by default)
   * @param {getDataHandler} options.getDataHandler Get data handler for processing related models
   * @param {getAddingMethod} options.getAddingMethod Get `add()` method of future `subscription`
   * @returns {SubscribeForData}
   */
  use,
};

/**
 * Assigns data from foreign model to target
 *
 * @callback assignData
 * @param {Object} target your target model
 * @param {Object} foreign foreign model
 */
/**
 * get unique identifier of target for internal indexing
 *
 * @callback getKey
 * @param {Object} target your target model
 * @return {*} target identifier
 */
/**
 * get unique identifier of target from foreign model
 *
 * @callback extractKey
 * @param {Object} foreign Foreign model data
 * @return {*} target identifier
 */
/**
 * get condition
 *
 * @callback getCondition
 * @param {Object} target your target model
 * @return {*} condition, can be scalar or object
 */
/**
 * get foreign data handler
 *
 * @callback getDataHandler
 * @param {Object} options Options
 * @param {Object} options.targets targets index
 * @param {String} options.targetField field data to be saved into
 * @param {extractKey} options.extractKey returns unique key of target model from foreign model
 * @param {Boolean} options.isMultiple if one to many relation
 * @param {String} options.sourceField field to use of foreign model
 * @param {assignData} options.assignData Do model filling by itself, otherwise use `targetField`
 * @return {Function} Callback handling data assignment
 */
/**
 * get future `subscription.add()` method
 *
 * @callback getAddingMethod
 * @param {Object} options Options
 * @param {Object} options.targets targets index
 * @param {getKey} options.getKey Callback which returns unique key from target model (model.id by default)
 * @param {getCondition} options.getCondition returns condition, using target model (model.id by default)
 * @param {*} options.defaultValue Default value for field
 * @param {String} options.targetField field data to be saved into
 * @param {object} options.condition DB Query condition, being prepared
 * @param {extractKey} options.extractKey returns unique key of target model from foreign model
 * @param {String} options.foreignField If `getCondition` returns scalar values this field will be used for $in
 * @param {Array} options.inner Internal array for condition storing
 * @return {Function} Callback handling data assignment
 */
/**
 * get stream from model using condition
 *
 * @callback getStream
 * @param source Source model
 * @param condition Query condition
 */
