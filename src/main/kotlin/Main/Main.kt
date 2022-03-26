import java.util.*
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

fun main() {
    val containerFactory = ContainerFactory(maxContainers = 8)
    val configurations = ('A'..'Z').map { it.toString() }

    val testsCount = 10000
    val testsPerConfiguration = (1..testsCount)
        .groupingBy { configurations[it % configurations.size] }
        .eachCount()

    println(testsPerConfiguration)

    val startTime = System.currentTimeMillis()

    val threads = (1..testsCount).map {
        Thread {
            val container = containerFactory.acquireContainer(configurations[it % configurations.size])
            // Pretend that thread is doing some long blocking work
            Thread.sleep((10000 + ThreadLocalRandom.current().nextLong(50000)))
            containerFactory.releaseContainer(container.containerId)
        }
    }
    threads.forEach { it.start() }
    threads.forEach { it.join() }

    println(System.currentTimeMillis() - startTime)
}

typealias Configuration = String
typealias ContainerId = String

class Container(val containerId: ContainerId) {

    enum class State { DISABLED, STARTING, RUNNING }

    private val lock = ReentrantLock()
    private var state = State.DISABLED

    fun start() = guardWithLock {
        when (state) {
            State.DISABLED -> init()
            State.RUNNING -> println("Container: $containerId is trying to be started by thread: ${Thread.currentThread().name}")
            else -> throw java.lang.IllegalArgumentException("Should never happen")
        }
    }

    private fun init() {
        state = State.STARTING
        println("Thread ${Thread.currentThread().name} is starting container: $containerId")
        Thread.sleep(5000)
        state = State.RUNNING
    }

    fun stop() = guardWithLock {
        Thread.sleep(2000)
    }

    private fun guardWithLock(block: () -> Unit): Unit =
        try {
            lock.lock()
            block()
        } finally {
            lock.unlock()
        }
}

class ContainerFactory(val maxContainers: Int = 2) {

    private val registryGuard: Lock = ReentrantLock(true)
    private val containerAddedOrReleasedOrStopped: Condition = registryGuard.newCondition()

    private val containerRegistry: MutableMap<Configuration, Container> = HashMap()
    private val stoppingContainers: MutableSet<ContainerId> = HashSet()
    private val usedContainers: MutableMap<ContainerId, AtomicLong> = HashMap()

    fun acquireContainer(configuration: Configuration): Container = guardWithLock {
        when {
            containerRegistry.containsKey(configuration) -> getExistingContainer(configuration)
            containerRegistry.size + stoppingContainers.size < maxContainers -> createContainer(configuration)
            else -> await(configuration).run { createOrGetExistingContainer(configuration) }
        }
    }.also { it.start() }

    private fun getExistingContainer(configuration: Configuration): Container =
        containerRegistry[configuration]!!.also {
            registerAsActive(it.containerId)
            println(
                "Thread ${Thread.currentThread().name}: Acquiring existing container ${it.containerId} for " +
                    "configuration $configuration"
            )
        }

    private fun createContainer(configuration: Configuration): Container =
        Container(UUID.randomUUID().toString()).also {
            containerRegistry[configuration] = it
            containerAddedOrReleasedOrStopped.signalAll()
            registerAsActive(it.containerId)
            println(
                "Thread ${Thread.currentThread().name}: Creating new container ${it.containerId} for configuration " +
                    configuration
            )
        }

    private fun registerAsActive(containerId: ContainerId): Long =
        usedContainers.computeIfAbsent(containerId) { AtomicLong(0) }.incrementAndGet()

    private fun createOrGetExistingContainer(configuration: Configuration): Container =
        when {
            // check if needed container has been added by another thread
            containerRegistry.containsKey(configuration) ->
                getExistingContainer(configuration)

            // check if we have empty room for new container
            containerRegistry.size + stoppingContainers.size < maxContainers ->
                createContainer(configuration)

            // check if we have a released container and we can remove it
            unusedContainers().isNotEmpty() -> {
                stopAndRemoveRandomUnusedContainer()
                // after previous operation, state could have changed, so we need to recheck everything
                // we will not get infinite recursion because on previous operation we have removed stopped containers
                // so at least containerRegistry.size + stoppingContainers.size < maxContainers condition will be satisfied
                createOrGetExistingContainer(configuration)
            }

            else -> throw IllegalStateException("Should never happen")
        }

    private fun await(configuration: Configuration) {
        while (
            containerRegistry.size + stoppingContainers.size == maxContainers
            && !containerRegistry.containsKey(configuration)
            && unusedContainers().isEmpty()
        ) {
            containerAddedOrReleasedOrStopped.await()
        }
    }

    private fun stopAndRemoveRandomUnusedContainer() {
        val containerId = unusedContainers().random()
        val container = containerRegistry.removeContainerRecord(containerId)

        if (container != null) {
            stoppingContainers.add(containerId)
            registryGuard.unlock()

            println("Thread ${Thread.currentThread().name}: Stopping and removing container $containerId")
            container.stop()

            registryGuard.lock()
            stoppingContainers.remove(containerId)
            containerAddedOrReleasedOrStopped.signalAll()
        }
    }

    fun releaseContainer(containerId: ContainerId) = guardWithLock {
        // If containerId is invalid
        if (!containerRegistry.values.any { it.containerId == containerId }
            && !usedContainers.containsKey(containerId)) {
            throw IllegalArgumentException("Invalid containerId: $containerId passed to be released")
        }

        val containerUsers = usedContainers[containerId]!!
        containerUsers.decrementAndGet()
        println(
            "Thread ${Thread.currentThread().name}: Releasing container $containerId, another users count " +
                "${containerUsers.get()}"
        )

        if (containerUsers.get() == 0L) {
            usedContainers.remove(containerId)
            containerAddedOrReleasedOrStopped.signalAll()
        }
    }

    private fun runningContainers(): Set<ContainerId> =
        containerRegistry.values.map { it.containerId }.toSet()

    private fun unusedContainers(): Set<ContainerId> =
        runningContainers() - usedContainers.keys

    private fun MutableMap<Configuration, Container>.removeContainerRecord(containerId: ContainerId): Container? =
        entries
            .firstOrNull { (_, entryContainer) -> entryContainer.containerId == containerId }
            ?.key
            .let(::remove)

    private fun <T> guardWithLock(block: () -> T) =
        try {
            registryGuard.lock()
            block()
        } finally {
            registryGuard.unlock()
        }
}
