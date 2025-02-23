use Nats;
use JSON::Fast;

unit class RedEventStore::Client;

has Nats $.nats .= new;

method TWEAK(|) { await $!nats.start }

method add-event($event) {
	my $type = $event.^name;
	my %data = $event.^attributes.grep(*.has_accessor).map: { .name.substr(2) => .get_value: $event }

	my @types = (|$event.^mro, |$event.^roles).map( *.^name ).grep: { $_ ne <Any Mu>.any };

	await $!nats.request: [ "add_event", |@types ].join("."), %data.&to-json
}

method get-events(Int $index = 0, :@types, Instant :$from-timestamp, Instant :$to-timestamp, *%pars) {
	my @events = .json given await $!nats.request:
		[ "get_events", $index, |@types ].join("."),
		%(
			|(:$from-timestamp with $from-timestamp),
			|(:$to-timestamp with $to-timestamp),
			|%pars
		).&to-json
	;

	gather for @events -> (Str :$type, UInt :$seq, :%data) {
		my $t = ::($type);
		if !$t && $t ~~ Failure {
			require ::($type);
			$t = ::($type);
		}
		$*SOURCING-MESSAGE-SEQ = $seq if $*SOURCING-MESSAGE-SEQ !~~ Failure;
		take $t.new: |%data
	}
}
