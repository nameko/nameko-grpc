# -*- coding: utf-8 -*-
import random
import time
from collections import defaultdict

import eventlet
import pytest

from nameko_grpc.utils import Teeable


class TestTeeable:
    @pytest.fixture
    def tracker(self):
        return []

    @pytest.fixture
    def make_generator(self, tracker):
        def gen():
            for i in range(10):
                tracker.append(i)
                yield i

        return gen

    def test_not_iterable(self):
        with pytest.raises(TypeError):
            Teeable(1)

    def test_tees_are_independent(self, make_generator):

        gen = Teeable(make_generator())
        tee = gen.tee()

        assert next(gen) == 0
        assert next(gen) == 1
        assert next(tee) == 0
        assert next(gen) == 2
        assert next(tee) == 1
        assert next(tee) == 2

    def test_generator_only_advances_once(self, make_generator, tracker):
        gen = Teeable(make_generator())
        tee = gen.tee()

        assert next(gen) == 0
        assert next(gen) == 1
        assert next(tee) == 0
        assert next(tee) == 1
        assert next(gen) == 2
        assert next(tee) == 2

        assert tracker == [0, 1, 2]

    def test_wrap_after_start(self, make_generator):
        generator = make_generator()

        assert next(generator) == 0
        assert next(generator) == 1

        gen = Teeable(generator)
        tee = gen.tee()

        assert next(gen) == 2
        assert next(tee) == 2
        assert next(gen) == 3
        assert next(tee) == 3

    def test_tee_after_start(self, make_generator):
        generator = make_generator()
        gen = Teeable(generator)

        assert next(generator) == 0
        assert next(generator) == 1

        tee = gen.tee()

        assert next(gen) == 2
        assert next(tee) == 2
        assert next(gen) == 3
        assert next(tee) == 3

    def test_reentrant(self, make_generator, tracker):

        gen = Teeable(make_generator())
        tee1 = gen.tee()
        tee2 = gen.tee()

        assert next(gen) == 0
        assert next(gen) == 1
        assert next(tee1) == 0
        assert next(tee1) == 1
        assert next(tee2) == 0
        assert next(tee2) == 1

    def test_thread_safe(self, make_generator, tracker):

        gen = Teeable(make_generator())
        tee = gen.tee()

        consume_trackers = defaultdict(list)

        def consume(iterable, ident=None):
            for i in iterable:
                time.sleep(random.random() / 10)
                consume_trackers[ident].append(i)

        gt1 = eventlet.spawn(consume, gen, ident="gen")
        gt2 = eventlet.spawn(consume, tee, ident="tee")

        gt1.wait()
        gt2.wait()

        assert consume_trackers["gen"] == list(range(10))
        assert consume_trackers["tee"] == list(range(10))
        assert tracker == list(range(10))

    def test_thread_safe_and_reentrant(self, make_generator, tracker):

        gen = Teeable(make_generator())
        tee1 = gen.tee()
        tee2 = gen.tee()
        tee3 = gen.tee()

        consume_trackers = defaultdict(list)

        def consume(iterable, ident=None):
            for i in iterable:
                time.sleep(random.random() / 10)
                consume_trackers[ident].append(i)

        gt1 = eventlet.spawn(consume, gen, ident="gen")
        gt2 = eventlet.spawn(consume, tee1, ident="tee1")
        gt3 = eventlet.spawn(consume, tee2, ident="tee2")
        gt4 = eventlet.spawn(consume, tee3, ident="tee3")

        gt1.wait()
        gt2.wait()
        gt3.wait()
        gt4.wait()

        assert consume_trackers["gen"] == list(range(10))
        assert consume_trackers["tee1"] == list(range(10))
        assert consume_trackers["tee2"] == list(range(10))
        assert consume_trackers["tee3"] == list(range(10))
        assert tracker == list(range(10))
