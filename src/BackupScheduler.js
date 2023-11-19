const Backup = require('./Backup');
const EventEmitter = require('events');

/**
 * @typedef {Object} BackupSchedulerOptions
 * @property {number} interval The interval (in milliseconds) between creating each backup.
 * @property {number} limit The maximum number of backups allowed in the upstream storage. Oldest backups will be deleted to make room for new backups.
 */

/**
 * @typedef {Object} BackupSchedulerRequiredMethods
 * @property {function():Backup[]|Promise<Backup[]>} list This method should return a list of all backups in the upstream storage.
 * @property {function():Backup|Promise<Backup>} create This method should create a new backup and upload it to the upstream storage, then retuern a Backup instance.
 * @property {function(Backup[]):void|Promise<void>} delete This method should delete the specified backups from the upstream storage.
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
            // List the current backups in the upstream storage
            const list = await this.options.list();
            if (!Array.isArray(list) || list.find((backup) => !(backup instanceof Backup)))
                throw new Error('BackupScheduler -> The "list" function must return an Array of "Backup" instances.');

            // Shallow copy and sort the backups by increasing creation date to ensure the oldest backups can be deleted first
            const sorted = [...list].sort((a, b) => a.created_at - b.created_at);

            // Create a new backup
            const backup = await this.options.create();
            if (!(backup instanceof Backup))
                throw new Error('BackupScheduler -> The "create" function must create and return a "Backup" instance.');

            // Check if the backup limit has been exceeded
            const overflow = sorted.length - this.options.limit + 1;
            if (overflow > 0) {
                // Slice the oldest backups which are over the limit
                const sliced = sorted.slice(0, overflow);

                // Delete the oldest backups
                await this.options.delete(sliced);
            }

            // Set the success flag
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
