// Define a backup class to hold each backup instance
class Backup {
    id;
    created_at;

    /**
     * Creates a new Backup instance.
     * @param {string} id A unique identifier for this backup. Use this to map a Backup instance to a backup data point in the upstream storage.
     * @param {number} created_at The UNIX timestamp of when this backup was created (in milliseconds).
     */
    constructor(id, created_at) {
        // Ensure a valid id is always provided
        if (!id || typeof id !== 'string')
            throw new Error('new Backup(id, created_at) -> Please provide a valid "id" to identify this backup.');

        // Ensure a created_at timestamp is always provided
        if (!created_at || typeof created_at !== 'number' || created_at < 1)
            throw new Error(
                'new Backup(id, created_at) -> Please provide a valid "created_at" timestamp (in milliseconds) for this backup.'
            );

        // Set the properties
        this.id = id;
        this.created_at = created_at;
    }
}

module.exports = Backup;
