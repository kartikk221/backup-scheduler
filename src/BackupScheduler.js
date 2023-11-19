const Backup = require('./Backup');
const EventEmitter = require('events');

/**
 * @typedef {Object} BackupSchedulerOptions
 * @property {number} interval The interval (in milliseconds) between creating each backup.
 * @property {number} limit The total number of backups to keep in the upstream storage.
 */

/**
 * @typedef {Object} BackupSchedulerRequiredMethods
 * @property {function(number):Backup[]|Promise<false|Backup[]>} list This method **must** return a list of all backups in the upstream storage.
 * - This method provides the `limit` as the first argument to allow you to return `false` if it can be confirmed that the upstream has not exceeded the `limit` yet based on size.
 * - This should only be used as an optimization to prevent unnecessary API calls to the upstream storage for additional information about each backup if we are sure there aren't enough backups to exceed the `limit`.
 * @property {function():void|Promise<void>} create This method **must** create a new backup and upload it to the upstream storage.
 * @property {function(Backup[]):void|Promise<void>} delete This method **must** delete the specified backups from the upstream storage.
 */

// Define a backup scheduler class to handle the backup process
class BackupScheduler extends EventEmitter {
    #interval;

    /**
     * @type {BackupSchedulerOptions}
     */
    options;

    /**
     * Creates a new BackupScheduler instance.
     * @param {BackupSchedulerOptions & BackupSchedulerRequiredMethods} options
     */
    constructor(options) {
        // If options is not a valid Object, throw an error
        if (!options || typeof options !== 'object')
            throw new Error('new BackupScheduler(options) -> Please provide a valid "options" Object.');

        // Ensure an interval is always provided
        if (!options.interval || typeof options.interval !== 'number' || options.interval < 1)
            throw new Error(
                'new BackupScheduler(options) -> Please provide a valid "interval" (in milliseconds) between each backup.'
            );

        // Ensure a limit is always provided
        if (!options.limit || typeof options.limit !== 'number' || options.limit < 1)
            throw new Error(
                'new BackupScheduler(options) -> Please provide a valid "limit" (in milliseconds) between each backup.'
            );

        // Ensure a list function is always provided
        if (!options.list || typeof options.list !== 'function')
            throw new Error(
                'new BackupScheduler(options) -> Please provide a valid "list" function to list the current backups in the upstream storage.'
            );

        // Ensure a create function is always provided
        if (!options.create || typeof options.create !== 'function')
            throw new Error(
                'new BackupScheduler(options) -> Please provide a valid "create" function to generate and upload a backup to the upstream storage.'
            );

        // Ensure a delete function is always provided
        if (!options.delete || typeof options.delete !== 'function')
            throw new Error(
                'new BackupScheduler(options) -> Please provide a valid "delete" function to delete a backup from the upstream storage.'
            );

        // Initialize the event emitter
        super();

        // Set the options
        this.options = options;

        // Create the interval to run the backup cycle
        this.#interval = setInterval(() => this.cycle(), this.options.interval);
    }

    #in_flight = false;
    /**
     * Runs a singular backup cycle.
     * - This method is automatically called at the specified interval by the scheduler.
     * - You should only call this method manually if you want to retry a failed backup cycle.
     * - **Note!** This method will not throw any errors, but an `error` event will be emitted if an error occurs.
     * - **Note!** This method will immediately return `false` if a backup cycle is already in progress.
     * @returns {Promise<boolean>} Whether or not this backup cycle was successful.
     */
    async cycle() {
        // If a backup cycle is already in progress, immediately return false
        if (this.#in_flight) return false;
        this.#in_flight = true;

        // Safely perform the backup cycle
        let success = false;
        try {
            // List the current backups
            // If the list function returns false, we can safely assume the limit has not been exceeded yet
            const list = await this.options.list();
            if (list === false) {
                // Create a new backup and don't worry about deleting any backups
                await this.options.create();
            } else {
                // Ensure the returned list is an Array of Backup instances
                if (!Array.isArray(list) || list.find((backup) => !(backup instanceof Backup)))
                    throw new Error(
                        'BackupScheduler -> The "list" function must return an Array of "Backup" instances.'
                    );

                // Shallow copy and sort the backups in oldest to newest order
                const sorted = [...list].sort((a, b) => a.created_at - b.created_at);

                // Create a new backup
                await this.options.create();

                // Check if the backup limit has been exceeded and if so by how many
                const overflow = sorted.length - this.options.limit + 1;
                if (overflow > 0) {
                    // Slice the oldest backups which are over the limit and delete them
                    const sliced = sorted.slice(0, overflow);
                    await this.options.delete(sliced);
                }
            }

            // Mark the backup cycle as successful
            success = true;
        } catch (error) {
            // Emit the error event
            this.emit('error', error);
        }

        this.#in_flight = false;
        return success;
    }

    /**
     * Destroys the backup scheduler and ensures no more backups are created.
     */
    destroy() {
        // Clear the interval
        clearInterval(this.#interval);
    }
}

module.exports = BackupScheduler;
