const awaiting = [];
const defaultOptions = {
  getKey: item => item._id,
  getCondition: item => item._id,
  getStream: (source, condition) => source.find(condition).lean().cursor(),
};

/**
 * Assigns data from foreign model to target
 *
 * @callback assignData
 * @param {Object} target your target model
 * @param {Object} foreign foreign data
 */

/**
 *
 * @param {Function} source Function to run on filling needed, it must return mongoose `Query`
 * @param {String} options.targetField field data to be saved into (optional)
 * @param {Object} options Options
 * @param {Object} options.baseCondition Base condition
 * @param {mixed} options.defaultValue Default value for field
 * @param {Function} options.getKey Callback which returns unique key from target model (model.id by default)
 * @param {Function} options.getStream returns stream from source and condition (using mongoose model by default)
 * @param {Function} options.getCondition returns condition, using target model (model.id by default)
 * @param {Function} options.extractKey returns unique key of target model from foreign model
 * @param {Boolean} options.isMultiple if one to many relation
 * @param {String} options.foreignField If `getCondition` returns scalar values this field will be used for $in
 * @param {String} options.sourceField field to use of foreign model
 * @param {assignData} options.assignData (optional) Do model filling by itself, otherwise use `targetField`
 * @returns {Object}
 */
function makeSubscription(source, options) {
  const {
    getKey, getCondition, foreignField, baseCondition, defaultValue, targetField,
  } = Object.assign({}, options, defaultOptions);
  options.extractKey = options.extractKey || (foreign => foreign[foreignField]);
  const targets = {};
  const condition = baseCondition || {};
  const inner = [];
  let isConditionSetup = false;
  awaiting.push({ source, targetField, options, targets, condition });

  return {
    add(target) {
      Object.assign(targets, { [getKey(target)]: target });
      const itemCondition = getCondition(target);

      if (defaultValue) if (typeof defaultValue !== 'object') target[targetField] = defaultValue;
      else Object.assign(target, defaultValue);
      if (!isConditionSetup) {
        if (typeof itemCondition === 'object') condition.$or = condition.$or || inner;
        else condition[foreignField] = condition[foreignField] || { $in: inner };

        isConditionSetup = true;
      }

      inner.push(itemCondition);
    },
  };
}

/**
 * Fill subscribed targets
 * @returns {Promise}
 */
function fillSubscriptions() {
  const promises = awaiting
    .map(({
      source, targetField, options: { extractKey, assignData, sourceField, getStream, isMultiple }, targets, condition,
    }) => new Promise((resolve, reject) => getStream(source, condition)
      .on('data', (foreign) => {
        const target = targets[extractKey(foreign)];

        if (assignData) return assignData(target, foreign);
        if (!isMultiple) target[targetField] = foreign[sourceField];
        else (target[targetField] = target[targetField] || []).push(foreign[sourceField]);
      })
      .on('error', reject)
      .on('end', resolve)));
  awaiting.splice(0, awaiting.length);

  return Promise.all(promises);
}

module.exports = {
  fillSubscriptions,
  makeSubscription,
  assignDefaultOptions(mixin) {
    Object.assign(defaultOptions, mixin);
  },
};
